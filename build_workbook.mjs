import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const root = new URL("./", import.meta.url);
const outputDir = new URL("output/", root);

function parseCsv(text) {
  return text
    .replace(/^\uFEFF/, "")
    .trim()
    .split(/\r?\n/)
    .map((line) => line.split(","));
}

function typedResultRows(rows) {
  return rows.map((row, rowIndex) =>
    row.map((value, columnIndex) => {
      if (rowIndex === 0) return value;
      if (columnIndex === 0) return new Date(`${value}T00:00:00Z`);
      if (value === "NA" || value === "") return "NA";
      if (columnIndex === row.length - 1) return value;
      const number = Number(value);
      return Number.isFinite(number) ? number : value;
    }),
  );
}

function setCommonSheetStyle(sheet) {
  sheet.showGridLines = false;
  sheet.getRange("A1:Z30").format.font = {
    name: "Arial",
    size: 10,
    color: "#1F2937",
  };
}

function addSummarySheet(workbook, summaryRows) {
  const sheet = workbook.worksheets.add("Summary");
  setCommonSheetStyle(sheet);
  sheet.tabColor = "#1F4E78";
  sheet.getRange("A2:G2").format.borders = {
    bottom: { style: "thin", color: "#8091A5" },
  };
  sheet.getRange("A2").values = [["指数拥挤度温度计 V1（盘后版）"]];
  sheet.getRange("A2").format.font = {
    name: "Arial",
    size: 14,
    bold: true,
    color: "#1F2937",
  };
  sheet.getRange("A3").values = [["温度仅描述市场拥挤状态，不构成投资指令。"]];
  sheet.getRange("A3:G3").format.font = {
    name: "Arial",
    size: 10,
    italic: true,
    color: "#4B5563",
  };
  const headers = ["指数代码", "指数名称", "数据日期", "当前温度", "较上一日变化", "温度区间", "数据状态"];
  const values = [
    headers,
    ...summaryRows.map((row) => [
      row["指数代码"],
      row["指数名称"],
      new Date(`${row["数据日期"]}T00:00:00Z`),
      row["当前温度"] ?? "NA",
      row["较上一日变化"] ?? "NA",
      row["温度区间"] ?? "NA",
      row["数据状态"],
    ]),
  ];
  sheet.getRangeByIndexes(4, 0, values.length, headers.length).values = values;
  const lastRow = 4 + values.length;
  sheet.getRange("A5:G5").format = {
    fill: "#1F4E78",
    font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    borders: {
      insideVertical: { style: "thin", color: "#FFFFFF" },
      bottom: { style: "medium", color: "#163A5C" },
    },
  };
  sheet.getRange("A5:G5").format.rowHeight = 28;
  sheet.getRange(`A6:G${lastRow}`).format.rowHeight = 24;
  sheet.getRange(`A6:G${lastRow}`).format.borders = {
    bottom: { style: "thin", color: "#D9E1E8" },
  };
  sheet.getRange(`C6:C${lastRow}`).setNumberFormat("yyyy-mm-dd");
  sheet.getRange(`D6:E${lastRow}`).setNumberFormat("0.0;[Red](0.0);-");
  sheet.getRange("A:A").format.columnWidth = 13;
  sheet.getRange("B:B").format.columnWidth = 26;
  sheet.getRange("C:C").format.columnWidth = 15;
  sheet.getRange("D:E").format.columnWidth = 18;
  sheet.getRange("F:G").format.columnWidth = 15;
  sheet.getRange(`A6:A${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange(`C6:G${lastRow}`).format.horizontalAlignment = "center";
}

function addResultSheet(workbook, config, rows) {
  const sheet = workbook.worksheets.add(config.code);
  setCommonSheetStyle(sheet);
  sheet.tabColor = "#5B9BD5";
  sheet.getRange("A2:R2").format.borders = {
    bottom: { style: "thin", color: "#8091A5" },
  };
  sheet.getRange("A2").values = [[`${config.name}（${config.code}）最近5个有效交易日温度`]];
  sheet.getRange("A2").format.font = {
    name: "Arial",
    size: 14,
    bold: true,
    color: "#1F2937",
  };
  sheet.getRange("A3").values = [[`官方行情最新日期：${config.date}；数据状态：${config.status}；缺失值保留为 NA。`]];
  sheet.getRange("A3:R3").format.font = {
    name: "Arial",
    size: 10,
    italic: true,
    color: "#4B5563",
  };
  sheet.getRange("A4").values = [[config.source]];
  sheet.getRange("A4:R4").format.font = {
    name: "Arial",
    size: 9,
    color: "#2563EB",
  };
  const values = typedResultRows(rows);
  const tableRange = sheet.getRangeByIndexes(5, 0, values.length, values[0].length);
  tableRange.values = values;
  tableRange.format.verticalAlignment = "center";
  const lastRow = 5 + values.length;
  sheet.getRange("A6:R6").format = {
    fill: "#1F4E78",
    font: { name: "Arial", size: 10, bold: true, color: "#FFFFFF" },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    wrapText: true,
    borders: {
      insideVertical: { style: "thin", color: "#FFFFFF" },
      bottom: { style: "medium", color: "#163A5C" },
    },
  };
  sheet.getRange("A6:R6").format.rowHeight = 44;
  sheet.getRange(`A7:R${lastRow}`).format.rowHeight = 22;
  sheet.getRange(`A6:R${lastRow}`).format.borders = {
    bottom: { style: "thin", color: "#D9E1E8" },
  };
  sheet.getRange(`A7:A${lastRow}`).setNumberFormat("yyyy-mm-dd");
  for (const column of ["B", "D", "F", "H", "J", "L", "N"]) {
    sheet.getRange(`${column}7:${column}${lastRow}`).setNumberFormat("0.00%;[Red](0.00%);-");
  }
  for (const column of ["C", "E", "G", "I", "K", "M", "O", "P", "Q"]) {
    sheet.getRange(`${column}7:${column}${lastRow}`).setNumberFormat("0.0;[Red](0.0);-");
  }
  sheet.getRange(`A7:A${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange(`B7:Q${lastRow}`).format.horizontalAlignment = "right";
  sheet.getRange(`R7:R${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange("A:A").format.columnWidth = 15;
  sheet.getRange("B:O").format.columnWidth = 18;
  sheet.getRange("P:Q").format.columnWidth = 19;
  sheet.getRange("R:R").format.columnWidth = 14;
  sheet.freezePanes.freezeRows(6);
  sheet.freezePanes.freezeColumns(1);
}

await fs.mkdir(outputDir, { recursive: true });
const summaryRows = JSON.parse(
  await fs.readFile(new URL("output/temperature_summary.json", root), "utf8"),
);
const workbook = Workbook.create();
addSummarySheet(workbook, summaryRows);

const sourceByCode = {
  "399006": "来源：国证指数官网（AKShare index_hist_cni）",
  "930986": "来源：中证指数官网（AKShare stock_zh_index_hist_csindex）",
};
for (const summary of summaryRows) {
  const code = summary["指数代码"];
  const csv = await fs.readFile(
    new URL(`output/index_temperature_${code}_latest5.csv`, root),
    "utf8",
  );
  addResultSheet(
    workbook,
    {
      code,
      name: summary["指数名称"],
      date: summary["数据日期"],
      status: summary["数据状态"],
      source: sourceByCode[code],
    },
    parseCsv(csv),
  );
}

workbook.recalculate();
for (const sheetName of ["Summary", "399006", "930986"]) {
  const range = sheetName === "Summary" ? "A2:G7" : "A2:R11";
  const inspected = await workbook.inspect({
    kind: "table",
    range: `${sheetName}!${range}`,
    include: "values,formulas",
    tableMaxRows: 12,
    tableMaxCols: 20,
    maxChars: 16000,
  });
  console.log(inspected.ndjson);
  const preview = await workbook.render({
    sheetName,
    range: `A1:${sheetName === "Summary" ? "G8" : "R11"}`,
    scale: 1.2,
    format: "png",
  });
  await fs.writeFile(
    new URL(`data/workbook_preview_${sheetName}.png`, root),
    new Uint8Array(await preview.arrayBuffer()),
  );
}

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 100 },
  summary: "最终公式错误扫描",
});
console.log(errors.ndjson);

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(fileURLToPath(new URL("output/index_temperature_latest5.xlsx", root)));
