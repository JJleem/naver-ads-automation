require("dotenv").config();

const axios = require("axios");
const crypto = require("crypto");
const { BigQuery } = require("@google-cloud/bigquery");

// ==========================================
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
// ==========================================

const bigquery = new BigQuery({ projectId: BQ_CONFIG.projectId });
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const getYesterday = () => {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().split("T")[0];
};

const TARGET_DATE = getYesterday();

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
    console.log(`✅ [저장완료] ${rows.length}건 저장됨`);
  } catch (e) {
    if (attempt < 5) {
      console.log(`⏳ 저장 실패, 재시도 중... (${attempt}/5)`);
      await sleep(2000);
      return insertToBigQueryWithRetry(rows, attempt + 1);
    } else {
      console.error(`💀 최종 저장 실패`);
    }
  }
}

async function main() {
  console.log(`🚀 [일일 자동화] ${TARGET_DATE} 데이터 수집 시작!`);

  try {
    await bigquery.query(
      `DELETE FROM \`${BQ_CONFIG.projectId}.${BQ_CONFIG.datasetId}.${BQ_CONFIG.tableId}\` WHERE date = '${TARGET_DATE}'`,
    );
    console.log(`🧹 기존 ${TARGET_DATE} 데이터 삭제 완료`);
  } catch (e) {}

  const campaigns = (await callApi("/ncc/campaigns")) || [];
  const adGroups = (await callApi("/ncc/adgroups")) || [];
  const campMap = {};
  const productMap = {};
  campaigns.forEach((c) => {
    campMap[c.nccCampaignId] = c.name;
    productMap[c.nccCampaignId] = c.campaignTp;
  });

  let rowsBuffer = [];

  for (let i = 0; i < adGroups.length; i++) {
    const group = adGroups[i];
    process.stdout.write(`\r진행률: ${i + 1}/${adGroups.length} `);

    const keywords =
      (await callApi(`/ncc/keywords?nccAdgroupId=${group.nccAdgroupId}`)) || [];
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
          JSON.stringify({ since: TARGET_DATE, until: TARGET_DATE }),
        );
        const stats = await callApi(
          `/stats?fields=${fields}&ids=${ids}&timeRange=${timeRange}`,
        );

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
            date: TARGET_DATE,
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

    const fieldsG = encodeURIComponent('["impCnt","clkCnt","salesAmt"]');
    const timeRangeG = encodeURIComponent(
      JSON.stringify({ since: TARGET_DATE, until: TARGET_DATE }),
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
          date: TARGET_DATE,
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

    if (rowsBuffer.length >= 1000) {
      await insertToBigQueryWithRetry(rowsBuffer);
      rowsBuffer = [];
    }
  }
  if (rowsBuffer.length > 0) await insertToBigQueryWithRetry(rowsBuffer);
  console.log(`\n🎉 [완료] ${TARGET_DATE} 데이터 수집 끝!`);
}

main();
