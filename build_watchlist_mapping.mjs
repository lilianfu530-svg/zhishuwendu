import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const root = new URL("./", import.meta.url);
const payload = JSON.parse(
  await fs.readFile(new URL("data/watchlist_mapping_payload.json", root), "utf8"),
);
const workbook = Workbook.create();
const fontName = "Microsoft YaHei";
const navy = "#1F4E78";
const pale = "#EAF2F8";
const border = "#D9E1E8";

function common(sheet, lastColumn) {
  sheet.showGridLines = false;
  sheet.getRange(`A1:${lastColumn}40`).format.font = {
    name: fontName,
    size: 10,
    color: "#1F2937",
  };
}

function title(sheet, text, subtitle, lastColumn) {
  sheet.getRange(`A2:${lastColumn}2`).format.borders = {
    bottom: { style: "thin", color: "#8091A5" },
  };
  sheet.getRange("A2").values = [[text]];
  sheet.getRange("A2").format.font = {
    name: fontName,
    size: 14,
    bold: true,
    color: "#1F2937",
  };
  sheet.getRange("A3").values = [[subtitle]];
  sheet.getRange(`A3:${lastColumn}3`).format.font = {
    name: fontName,
    size: 10,
    color: "#4B5563",
  };
}

function header(range) {
  range.format = {
    fill: navy,
    font: { name: fontName, size: 10, bold: true, color: "#FFFFFF" },
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

const mapping = workbook.worksheets.add("target_index_mapping");
mapping.tabColor = navy;
common(mapping, "Q");
title(
  mapping,
  "指数温度计自选池映射与可行性核验",
  "第一阶段仅做身份核验和小范围接口实测；21个候选、3个基金排除；未运行新增指数温度计算。",
  "Q",
);
mapping.getRange("A5:D5").values = [[
  "EXACT",
  payload.summary.exact_count,
  "PUBLIC_EQUIVALENT",
  payload.summary.public_equivalent_count,
]];
mapping.getRange("F5:I5").values = [[
  "UNSUPPORTED",
  payload.summary.unsupported_count,
  "第二阶段候选",
  payload.summary.included_count,
]];
mapping.getRange("A5:I5").format.borders = {
  bottom: { style: "thin", color: border },
};
for (const column of ["A", "C", "F", "H"]) {
  mapping.getRange(`${column}5`).format.fill = pale;
  mapping.getRange(`${column}5`).format.font = {
    name: fontName,
    size: 10,
    bold: true,
    color: "#1F2937",
  };
}

const mappingHeaders = [
  "用户自选名称",
  "截图代码",
  "实际使用代码",
  "实际指数名称",
  "类别",
  "数据源",
  "映射状态",
  "纳入/排除",
  "排除原因",
  "约3年日行情",
  "当前成分股",
  "当前成分股数量",
  "成分股历史路径",
  "7项指标字段",
  "来源稳定性",
  "核验说明",
  "来源链接",
];
mapping.getRange("A7:Q7").values = [mappingHeaders];
mapping.getRange("B8:C28").setNumberFormat("@");
const mappingValues = payload.mappings.map((row) => [
  row.display_name,
  row.original_code,
  row.canonical_code,
  row.canonical_name,
  row.category,
  row.data_source,
  row.mapping_status,
  row.included,
  row.exclusion_reason,
  row.daily_history_check,
  row.current_constituents_check,
  row.current_constituents_count,
  row.member_history_check,
  row.seven_metrics_check,
  row.source_stability_check,
  `${row.mapping_note} ${row.daily_history_note} ${row.member_history_note}`,
  row.source_url,
]);
mapping.getRangeByIndexes(7, 0, mappingValues.length, mappingHeaders.length).values = mappingValues;
for (let index = 0; index < payload.mappings.length; index += 1) {
  const code = payload.mappings[index].canonical_code;
  if (/^0\d+$/.test(code)) {
    mapping.getRange(`C${index + 8}`).formulas = [[`="${code}"`]];
  }
}
header(mapping.getRange("A7:Q7"));
mapping.getRange("A8:Q28").format.rowHeight = 24;
mapping.getRange("A8:Q28").format.borders = {
  bottom: { style: "thin", color: border },
};
for (let index = 0; index < payload.mappings.length; index += 1) {
  const rowNumber = index + 8;
  if (payload.mappings[index].mapping_status === "UNSUPPORTED") {
    mapping.getRange(`G${rowNumber}`).format.fill = "#FCE8E6";
    mapping.getRange(`G${rowNumber}`).format.font = {
      name: fontName,
      size: 10,
      bold: true,
      color: "#B91C1C",
    };
  }
}
mapping.getRange("A:A").format.columnWidth = 18;
mapping.getRange("B:B").format.columnWidth = 19;
mapping.getRange("C:C").format.columnWidth = 22;
mapping.getRange("D:D").format.columnWidth = 26;
mapping.getRange("E:E").format.columnWidth = 14;
mapping.getRange("F:F").format.columnWidth = 91;
mapping.getRange("G:H").format.columnWidth = 20;
mapping.getRange("I:I").format.columnWidth = 46;
mapping.getRange("J:O").format.columnWidth = 17;
mapping.getRange("P:P").format.columnWidth = 90;
mapping.getRange("Q:Q").format.columnWidth = 85;
mapping.freezePanes.freezeRows(7);
mapping.freezePanes.freezeColumns(4);

const feasibility = workbook.worksheets.add("Data_Feasibility");
feasibility.tabColor = "#5B9BD5";
common(feasibility, "L");
title(
  feasibility,
  "公开数据可行性明细",
  "“通过”表示第一阶段小范围核验，非新增指数全量历史抓取已完成。历史成分只允许 current_constituents_proxy。",
  "L",
);
const feasibilityHeaders = [
  "用户自选名称",
  "截图代码",
  "映射状态",
  "约3年日行情",
  "行情核验说明",
  "当前成分股",
  "成分股数量",
  "成分股数据日期",
  "样本成分股历史",
  "7项指标字段",
  "来源稳定性",
  "核验说明",
];
feasibility.getRange("A5:L5").values = [feasibilityHeaders];
feasibility.getRange("B6:B26").setNumberFormat("@");
feasibility.getRangeByIndexes(5, 0, payload.mappings.length, feasibilityHeaders.length).values =
  payload.mappings.map((row) => [
    row.display_name,
    row.original_code,
    row.mapping_status,
    row.daily_history_check,
    row.daily_history_note,
    row.current_constituents_check,
    row.current_constituents_count,
    row.current_constituents_date ?? "NA",
    row.member_history_note,
    row.seven_metrics_check,
    row.source_stability_check,
    row.exclusion_reason || row.mapping_note,
  ]);
header(feasibility.getRange("A5:L5"));
feasibility.getRange("A6:L26").format.rowHeight = 25;
feasibility.getRange("A6:L26").format.borders = {
  bottom: { style: "thin", color: border },
};
feasibility.getRange("A:A").format.columnWidth = 18;
feasibility.getRange("B:D").format.columnWidth = 19;
feasibility.getRange("E:E").format.columnWidth = 38;
feasibility.getRange("F:H").format.columnWidth = 20;
feasibility.getRange("I:I").format.columnWidth = 51;
feasibility.getRange("J:K").format.columnWidth = 20;
feasibility.getRange("L:L").format.columnWidth = 83;
feasibility.freezePanes.freezeRows(5);
feasibility.freezePanes.freezeColumns(2);

const excluded = workbook.worksheets.add("Excluded_Funds");
excluded.tabColor = "#A5A5A5";
common(excluded, "D");
title(excluded, "明确排除的基金产品", "以下3项不计入21个候选，不参与映射状态统计。", "D");
excluded.getRange("A5:D5").values = [["截图名称", "截图代码", "产品类型", "排除原因"]];
excluded.getRange("B6:B8").setNumberFormat("@");
excluded.getRange("A6:D8").values = payload.excluded_funds.map((row) => [
  row.display_name,
  row.original_code,
  row.product_type,
  row.reason,
]);
header(excluded.getRange("A5:D5"));
excluded.getRange("A6:D8").format.rowHeight = 25;
excluded.getRange("A6:D8").format.borders = {
  bottom: { style: "thin", color: border },
};
excluded.getRange("A:A").format.columnWidth = 29;
excluded.getRange("B:C").format.columnWidth = 20;
excluded.getRange("D:D").format.columnWidth = 48;

workbook.recalculate();
const inspectionRanges = {
  target_index_mapping: "A2:I28",
  Data_Feasibility: "A2:L26",
  Excluded_Funds: "A2:D8",
};
const inspections = [];
for (const [sheetName, range] of Object.entries(inspectionRanges)) {
  const inspectResult = await workbook.inspect({
    kind: "table",
    range: `${sheetName}!${range}`,
    include: "values,formulas",
    tableMaxRows: 30,
    tableMaxCols: 17,
    maxChars: 30000,
  });
  inspections.push(inspectResult.ndjson);
  const preview = await workbook.render({
    sheetName,
    range,
    scale: 1.1,
    format: "png",
  });
  await fs.writeFile(
    new URL(`data/watchlist_mapping_${sheetName}.png`, root),
    new Uint8Array(await preview.arrayBuffer()),
  );
}
await fs.writeFile(
  new URL("data/watchlist_mapping_inspect.ndjson", root),
  inspections.join("\n"),
  "utf8",
);

const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!",
  options: { useRegex: true, maxResults: 300 },
  summary: "自选池映射工作簿公式错误扫描",
});
await fs.writeFile(
  new URL("data/watchlist_mapping_errors.ndjson", root),
  errors.ndjson,
  "utf8",
);

const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(fileURLToPath(new URL("output/watchlist_mapping.xlsx", root)));
