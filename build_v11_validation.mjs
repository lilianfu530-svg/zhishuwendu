import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const root = new URL("./", import.meta.url);
const payload = JSON.parse(
  await fs.readFile(new URL("data/temperature_v11_validation_payload.json", root), "utf8"),
);

const navy = "#1F4E78";
const blue = "#D9EAF7";
const paleBlue = "#EAF2F8";
const border = "#D9E1E8";
const text = "#1F2937";
const muted = "#4B5563";
const warningFill = "#FCE8E6";
const warningText = "#B91C1C";

function formatDate(value) {
  return typeof value === "string" ? value.slice(0, 10) : value;
}

function setBaseStyle(sheet, maxColumn) {
  sheet.showGridLines = false;
  sheet.getRange(`A1:${maxColumn}40`).format.font = {
    name: "Microsoft YaHei",
    size: 10,
    color: text,
  };
}

function setTitle(sheet, title, subtitle, maxColumn) {
  sheet.getRange(`A2:${maxColumn}2`).format.borders = {
    bottom: { style: "thin", color: "#8091A5" },
  };
  sheet.getRange("A2").values = [[title]];
  sheet.getRange("A2").format.font = {
    name: "Microsoft YaHei",
    size: 14,
    bold: true,
    color: text,
  };
  sheet.getRange("A3").values = [[subtitle]];
  sheet.getRange(`A3:${maxColumn}3`).format.font = {
    name: "Microsoft YaHei",
    size: 9,
    color: muted,
  };
}

function styleHeader(range) {
  range.format = {
    fill: navy,
    font: { name: "Microsoft YaHei", size: 10, bold: true, color: "#FFFFFF" },
    horizontalAlignment: "center",
    verticalAlignment: "center",
    wrapText: true,
    borders: {
      insideVertical: { style: "thin", color: "#FFFFFF" },
      bottom: { style: "medium", color: "#163A5C" },
    },
  };
  range.format.rowHeight = 42;
}

function addVersionSheet(workbook, sheetName, data) {
  const sheet = workbook.worksheets.add(sheetName);
  sheet.tabColor = data.formula_version === "V1.1" ? "#5B9BD5" : "#8091A5";
  setBaseStyle(sheet, "S");
  setTitle(
    sheet,
    `${data.index_code} ${data.formula_version} 温度状态验证`,
    "收益、最大回撤和最大上涨均按指数收盘价计算；变化不足5个交易日的记录不进入状态样本。",
    "S",
  );
  const metadata = [
    ["公式版本", data.formula_version, "成分股模式", data.constituent_mode],
    ["有效温度区间", `${formatDate(data.start_date)} 至 ${formatDate(data.end_date)}`, "有效温度样本", data.valid_temperature_rows],
    ["已分类状态样本", data.classified_rows, "温度变化定义", "Temperature(t) - Temperature(t-5)"],
  ];
  sheet.getRange("A5:D7").values = metadata;
  sheet.getRange("A5:D7").format.borders = { preset: "all", style: "thin", color: border };
  sheet.getRange("A5:A7").format.fill = paleBlue;
  sheet.getRange("C5:C7").format.fill = paleBlue;
  sheet.getRange("A5:A7").format.font = { name: "Microsoft YaHei", size: 10, bold: true, color: text };
  sheet.getRange("C5:C7").format.font = { name: "Microsoft YaHei", size: 10, bold: true, color: text };

  const headers = [
    "状态",
    "状态样本数",
    "未来5日样本数",
    "未来5日平均收益",
    "未来5日中位收益",
    "未来5日上涨比例",
    "未来10日样本数",
    "未来10日平均收益",
    "未来10日中位收益",
    "未来10日上涨比例",
    "未来20日样本数",
    "未来20日平均收益",
    "未来20日中位收益",
    "未来20日上涨比例",
    "未来20日极值样本数",
    "未来20日平均最大回撤",
    "未来20日中位最大回撤",
    "未来20日平均最大上涨",
    "未来20日中位最大上涨",
  ];
  const rows = data.statistics.map((row) => headers.map((header) => row[header] ?? "NA"));
  sheet.getRange("A9:S9").values = [headers];
  sheet.getRangeByIndexes(9, 0, rows.length, headers.length).values = rows;
  styleHeader(sheet.getRange("A9:S9"));
  const lastRow = 9 + rows.length;
  sheet.getRange(`A10:S${lastRow}`).format.borders = {
    bottom: { style: "thin", color: border },
  };
  sheet.getRange(`A10:S${lastRow}`).format.rowHeight = 23;
  sheet.getRange(`B10:C${lastRow}`).setNumberFormat("#,##0");
  sheet.getRange(`G10:G${lastRow}`).setNumberFormat("#,##0");
  sheet.getRange(`K10:K${lastRow}`).setNumberFormat("#,##0");
  sheet.getRange(`O10:O${lastRow}`).setNumberFormat("#,##0");
  for (const column of ["D", "E", "F", "H", "I", "J", "L", "M", "N", "P", "Q", "R", "S"]) {
    sheet.getRange(`${column}10:${column}${lastRow}`).setNumberFormat("0.00%;[Red](0.00%);-");
  }
  sheet.getRange(`A10:A${lastRow}`).format.font = {
    name: "Microsoft YaHei",
    size: 10,
    bold: true,
    color: text,
  };
  sheet.getRange(`B10:S${lastRow}`).format.horizontalAlignment = "right";
  sheet.getRange("A:A").format.columnWidth = 18;
  for (const column of ["B", "C", "G", "K", "O"]) {
    sheet.getRange(`${column}:${column}`).format.columnWidth = 16;
  }
  for (const column of ["D", "E", "F", "H", "I", "J", "L", "M", "N", "P", "Q", "R", "S"]) {
    sheet.getRange(`${column}:${column}`).format.columnWidth = 18;
  }
  sheet.freezePanes.freezeRows(9);
  sheet.freezePanes.freezeColumns(1);
}

function addComparisonSheet(workbook, rows) {
  const sheet = workbook.worksheets.add("V1_vs_V1.1");
  sheet.tabColor = "#70AD47";
  setBaseStyle(sheet, "F");
  setTitle(
    sheet,
    "V1.0 与 V1.1 状态表现比较",
    "差值为 V1.1 减 V1.0。样本数用于判断结果稳定性，不以单个收益指标替代完整比较。",
    "F",
  );
  const headers = ["指数代码", "状态", "指标", "V1.0", "V1.1", "V1.1-V1.0"];
  const values = rows.map((row) => headers.map((header) => row[header] ?? "NA"));
  sheet.getRange("A5:F5").values = [headers];
  sheet.getRangeByIndexes(5, 0, values.length, headers.length).values = values;
  styleHeader(sheet.getRange("A5:F5"));
  const lastRow = 5 + values.length;
  sheet.getRange(`A6:F${lastRow}`).format.borders = {
    bottom: { style: "thin", color: border },
  };
  sheet.getRange(`A6:F${lastRow}`).format.rowHeight = 21;
  for (let index = 0; index < rows.length; index += 1) {
    const excelRow = 6 + index;
    const isCount = rows[index]["指标"].includes("样本数");
    sheet.getRange(`D${excelRow}:F${excelRow}`).setNumberFormat(
      isCount ? "#,##0;[Red](#,##0);-" : "0.00%;[Red](0.00%);-",
    );
    if (index % 18 === 0) {
      sheet.getRange(`A${excelRow}:F${excelRow}`).format.borders = {
        top: { style: "medium", color: "#9FBAD0" },
        bottom: { style: "thin", color: border },
      };
    }
  }
  sheet.getRange(`A6:B${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange(`D6:F${lastRow}`).format.horizontalAlignment = "right";
  sheet.getRange("A:A").format.columnWidth = 13;
  sheet.getRange("B:B").format.columnWidth = 19;
  sheet.getRange("C:C").format.columnWidth = 30;
  sheet.getRange("D:F").format.columnWidth = 17;
  sheet.freezePanes.freezeRows(5);
}

function addCorrelationSheet(workbook, rows, highPairs) {
  const sheet = workbook.worksheets.add("Dimension_Correlation");
  sheet.tabColor = "#ED7D31";
  setBaseStyle(sheet, "I");
  setTitle(
    sheet,
    "V1.1 四维度相关性",
    "分别计算 Pearson 和 Spearman；绝对相关性不低于0.80的组合仅报告，不修改模型。",
    "I",
  );
  sheet.getRange("A5:B5").values = [["高相关组合数", highPairs.length]];
  sheet.getRange("A5:B5").format.borders = { preset: "all", style: "thin", color: border };
  sheet.getRange("A5").format.fill = paleBlue;
  sheet.getRange("A5").format.font = { name: "Microsoft YaHei", size: 10, bold: true, color: text };
  const headers = [
    "指数代码",
    "方法",
    "维度1",
    "维度2",
    "相关系数",
    "绝对值不低于0.80",
    "有效样本数",
    "开始日期",
    "结束日期",
  ];
  const values = rows.map((row) =>
    headers.map((header) => {
      if (header === "开始日期" || header === "结束日期") {
        return new Date(`${formatDate(row[header])}T00:00:00Z`);
      }
      return row[header] ?? "NA";
    }),
  );
  sheet.getRange("A7:I7").values = [headers];
  sheet.getRangeByIndexes(7, 0, values.length, headers.length).values = values;
  styleHeader(sheet.getRange("A7:I7"));
  const lastRow = 7 + values.length;
  sheet.getRange(`A8:I${lastRow}`).format.borders = {
    bottom: { style: "thin", color: border },
  };
  sheet.getRange(`A8:I${lastRow}`).format.rowHeight = 22;
  sheet.getRange(`E8:E${lastRow}`).setNumberFormat("0.000");
  sheet.getRange(`G8:G${lastRow}`).setNumberFormat("#,##0");
  sheet.getRange(`H8:I${lastRow}`).setNumberFormat("yyyy-mm-dd");
  for (let index = 0; index < rows.length; index += 1) {
    if (rows[index]["绝对值不低于0.80"] === "是") {
      const excelRow = 8 + index;
      sheet.getRange(`A${excelRow}:I${excelRow}`).format.fill = warningFill;
      sheet.getRange(`E${excelRow}:F${excelRow}`).format.font = {
        name: "Microsoft YaHei",
        size: 10,
        bold: true,
        color: warningText,
      };
    }
  }
  sheet.getRange(`A8:B${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange(`E8:I${lastRow}`).format.horizontalAlignment = "center";
  sheet.getRange("A:B").format.columnWidth = 14;
  sheet.getRange("C:D").format.columnWidth = 21;
  sheet.getRange("E:E").format.columnWidth = 15;
  sheet.getRange("F:F").format.columnWidth = 20;
  sheet.getRange("G:G").format.columnWidth = 15;
  sheet.getRange("H:I").format.columnWidth = 15;
  sheet.freezePanes.freezeRows(7);
}

const workbook = Workbook.create();
for (const sheetName of ["399006_V1.0", "399006_V1.1", "930986_V1.0", "930986_V1.1"]) {
  addVersionSheet(workbook, sheetName, payload.version_sheets[sheetName]);
}
addComparisonSheet(workbook, payload.comparison);
addCorrelationSheet(workbook, payload.dimension_correlation, payload.high_correlation_pairs);

workbook.recalculate();

const inspections = [];
for (const sheetName of [
  "399006_V1.0",
  "399006_V1.1",
  "930986_V1.0",
  "930986_V1.1",
  "V1_vs_V1.1",
  "Dimension_Correlation",
]) {
  const range = sheetName === "V1_vs_V1.1" ? "A2:F30" : sheetName === "Dimension_Correlation" ? "A2:I32" : "A2:S15";
  const inspected = await workbook.inspect({
    kind: "table",
    range: `${sheetName}!${range}`,
    include: "values,formulas",
    tableMaxRows: 32,
    tableMaxCols: 20,
    maxChars: 20000,
  });
  inspections.push(inspected.ndjson);
  const preview = await workbook.render({
    sheetName,
    range,
    scale: 1.1,
    format: "png",
  });
  await fs.writeFile(
    new URL(`data/temperature_v11_${sheetName.replaceAll(".", "_")}.png`, root),
    new Uint8Array(await preview.arrayBuffer()),
  );
}
await fs.writeFile(
  new URL("data/temperature_v11_validation_inspect.ndjson", root),
  inspections.join("\n"),
  "utf8",
);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 300 },
  summary: "V1.1验证工作簿公式错误扫描",
});
await fs.writeFile(
  new URL("data/temperature_v11_validation_errors.ndjson", root),
  errors.ndjson,
  "utf8",
);

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(fileURLToPath(new URL("output/temperature_v11_validation.xlsx", root)));
