require("dotenv").config(); // 👈 환경변수 로드 (필수)

const axios = require("axios");
const crypto = require("crypto");
const { BigQuery } = require("@google-cloud/bigquery");

// ==========================================
// ⚙️ [설정] .env 파일에서 보안 키를 가져옵니다.
const NAVER_CONFIG = {
  ACCESS_LICENSE: process.env.NAVER_ACCESS_LICENSE,
  SECRET_KEY: process.env.NAVER_SECRET_KEY,
  CUSTOMER_ID: process.env.NAVER_CUSTOMER_ID,
};

const BQ_CONFIG = {
  projectId: process.env.BQ_PROJECT_ID,
  datasetId: process.env.BQ_DATASET_ID,
  tableId: process.env.BQ_TABLE_ID,
};

// ⚠️ 날짜 설정은 로직의 일부이므로 코드에 남겨둡니다 (필요시 수정해서 쓰세요)
const START_DATE = "2025-06-05";
const END_DATE = "2026-01-09";
// ==========================================

const bigquery = new BigQuery({ projectId: BQ_CONFIG.projectId });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getSignature = (timestamp, method, path) => {
  return crypto
    .createHmac("sha256", NAVER_CONFIG.SECRET_KEY)
    .update(`${timestamp}.${method}.${path}`)
    .digest("base64");
};

async function callApi(path, method = "GET") {
  const url = `https://api.searchad.naver.com${path}`;
  const timestamp = Date.now().toString();
  const signature = getSignature(timestamp, method, path.split("?")[0]);
  try {
    const response = await axios({
      method: method,
      url: url,
      headers: {
        "X-Timestamp": timestamp,
        "X-API-KEY": NAVER_CONFIG.ACCESS_LICENSE,
        "X-Customer": NAVER_CONFIG.CUSTOMER_ID,
        "X-Signature": signature,
      },
    });
    return response.data;
  } catch (error) {
    if (error.response && error.response.status === 429) {
      await sleep(5000);
      return callApi(path, method);
    }
    return null;
  }
}

async function insertToBigQueryWithRetry(rows, attempt = 1) {
  try {
    await bigquery
      .dataset(BQ_CONFIG.datasetId)
      .table(BQ_CONFIG.tableId)
      .insert(rows);
  } catch (e) {
    // 에러 내용을 정확히 봅니다.
    const errorMsg = e.errors ? JSON.stringify(e.errors) : e.message;
    console.error(`\n❌ [저장실패] ${attempt}차 시도 실패: ${errorMsg}`);

    if (attempt < 5) {
      // 최대 5번까지 재시도
      console.log(`⏳ 2초 후 재시도합니다...`);
      await sleep(2000);
      return insertToBigQueryWithRetry(rows, attempt + 1);
    } else {
      console.error(
        `💀 [최종실패] 데이터 ${rows.length}건을 저장하지 못했습니다.`,
      );
    }
  }
}

async function main() {
  console.log("🚀 [재시도 기능 탑재] 데이터 수집 시작!");
  const campaigns = (await callApi("/ncc/campaigns")) || [];
  const adGroups = (await callApi("/ncc/adgroups")) || [];
  const campMap = {};
  const productMap = {};
  campaigns.forEach((c) => {
    campMap[c.nccCampaignId] = c.name;
    productMap[c.nccCampaignId] = c.campaignTp;
  });

  let currentDate = new Date(START_DATE);
  const endDate = new Date(END_DATE);

  while (currentDate <= endDate) {
    const dateStr = currentDate.toISOString().split("T")[0];
    console.log(`\n📅 [${dateStr}] 작업 중...`);

    // 초기화 (에러 나도 무시하고 진행)
    try {
      await bigquery.query(
        `DELETE FROM \`${BQ_CONFIG.projectId}.${BQ_CONFIG.datasetId}.${BQ_CONFIG.tableId}\` WHERE date = '${dateStr}'`,
      );
    } catch (e) {}

    let rowsBuffer = [];
    for (let i = 0; i < adGroups.length; i++) {
      const group = adGroups[i];
      process.stdout.write(
        `\r진행률: ${i + 1}/${adGroups.length} (${group.name.substring(0, 10)}) `,
      );

      const keywords =
        (await callApi(`/ncc/keywords?nccAdgroupId=${group.nccAdgroupId}`)) ||
        [];
      const keywordMap = {};
      const keywordIds = keywords.map((k) => {
        keywordMap[k.nccKeywordId] = k.keyword;
        return k.nccKeywordId;
      });

      let groupSum = { imp: 0, clk: 0, cost: 0 };
      if (keywordIds.length > 0) {
        for (let k = 0; k < keywordIds.length; k += 20) {
          const chunkIds = keywordIds.slice(k, k + 20);
          const fields = encodeURIComponent(
            JSON.stringify(["impCnt", "clkCnt", "salesAmt", "ctr", "cpc"]),
          );
          const ids = encodeURIComponent(chunkIds.join(","));
          const timeRange = encodeURIComponent(
            JSON.stringify({ since: dateStr, until: dateStr }),
          );
          const params = [
            `fields=${fields}`,
            `ids=${ids}`,
            `timeRange=${timeRange}`,
          ]
            .sort()
            .join("&");
          const stats = await callApi(`/stats?${params}`);
          const statMap = {};
          if (stats && stats.data)
            stats.data.forEach((s) => {
              statMap[s.id] = s;
            });

          chunkIds.forEach((id) => {
            const s = statMap[id] || {};
            groupSum.imp += s.impCnt || 0;
            groupSum.clk += s.clkCnt || 0;
            groupSum.cost += s.salesAmt || 0;
            rowsBuffer.push({
              date: dateStr,
              product: productMap[group.nccCampaignId] || "Unknown",
              campaign_name: campMap[group.nccCampaignId] || "Unknown",
              adgroup_name: group.name,
              keyword_name: keywordMap[id],
              impCnt: s.impCnt || 0,
              clkCnt: s.clkCnt || 0,
              ctr: s.ctr || 0,
              cpc: s.cpc || 0,
              salesAmt: s.salesAmt || 0,
              ingested_at: BigQuery.datetime(new Date().toISOString()),
            });
          });
          await sleep(50);
        }
      }

      // 자동확장
      const fieldsG = encodeURIComponent('["impCnt","clkCnt","salesAmt"]');
      const timeRangeG = encodeURIComponent(
        JSON.stringify({ since: dateStr, until: dateStr }),
      );
      const gStats = await callApi(
        `/stats?fields=${fieldsG}&ids=${group.nccAdgroupId}&timeRange=${timeRangeG}`,
      );
      if (gStats && gStats.data && gStats.data[0]) {
        const gs = gStats.data[0];
        const oImp = (gs.impCnt || 0) - groupSum.imp;
        const oClk = (gs.clkCnt || 0) - groupSum.clk;
        const oCost = (gs.salesAmt || 0) - groupSum.cost;
        if (oImp > 0 || oClk > 0 || oCost > 0) {
          rowsBuffer.push({
            date: dateStr,
            product: productMap[group.nccCampaignId] || "Unknown",
            campaign_name: campMap[group.nccCampaignId] || "Unknown",
            adgroup_name: group.name,
            keyword_name: "자동확장_기타",
            impCnt: oImp,
            clkCnt: oClk,
            ctr: 0,
            cpc: 0,
            salesAmt: oCost,
            ingested_at: BigQuery.datetime(new Date().toISOString()),
          });
        }
      }

      // 1000개 찰 때마다 저장 (재시도 로직 포함)
      if (rowsBuffer.length >= 1000) {
        await insertToBigQueryWithRetry(rowsBuffer);
        rowsBuffer = [];
      }
    }
    // 남은 거 저장
    if (rowsBuffer.length > 0) await insertToBigQueryWithRetry(rowsBuffer);
    currentDate.setDate(currentDate.getDate() + 1);
  }
}

main();
