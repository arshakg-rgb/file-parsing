const fs = require("fs");
const path = require("path");
const ts = require("typescript");

const projectRoot = path.join(__dirname, "..");
const srcDir = path.join(projectRoot, "src");

// import source paths are relative to src/
const importSource = {
  Logger: "utils/logger/logger.js",
  Request: "express",
  Response: "express",
  Firestore: "@google-cloud/firestore",
  ServiceManager: "config/ServiceManager.js",
  JobCounts: "shared/models/job.js",
  JobTimings: "shared/models/job.js",
  ClassifyRequest: "shared/models/template.js",
  ClassifyResponse: "shared/models/template.js",
  AIVerdict: "shared/models/template.js",
  Template: "shared/models/template.js",
  RecordTemplate: "shared/TemplateRegistryService.js",
  RubbishTemplate: "shared/TemplateRegistryService.js",
  TemplateKind: "shared/TemplateRegistryService.js",
  FieldLocator: "shared/models/template.js",
  ParquetRow: "services/job_service/finalize/ParquetEngine.js",
  TraceRecord: "shared/TraceSystem.js",
  SQSClientConfig: "@aws-sdk/client-sqs",
  SendMessageCommandInput: "@aws-sdk/client-sqs",
  ReceiveMessageCommandInput: "@aws-sdk/client-sqs",
  Message: "@aws-sdk/client-sqs",
  PutLogEventsCommandInput: "@aws-sdk/client-cloudwatch-logs",
  InputLogEvent: "@aws-sdk/client-cloudwatch-logs",
  PoolClient: "pg",
  OutputPartCreationAttributes: "config/db/models/OutputPart.js",
  DeadLetterAttributes: "config/db/models/DeadLetter.js",
  ParseJobAttributes: "config/db/models/ParseJob.js",
  ParseJobRow: "shared/DatabaseManager.js",
  OutputPartRow: "shared/DatabaseManager.js",
};

function relativeImport(fromFile, srcRelative) {
  const fromDir = path.dirname(fromFile).replace(srcDir + "/", "");
  if (srcRelative.startsWith("@") || !srcRelative.endsWith(".js")) return srcRelative;
  const target = srcRelative.replace(/\.js$/, "");
  let rel = path.relative(fromDir, target);
  if (!rel.startsWith(".")) rel = "./" + rel;
  return rel + ".js";
}

function findNodeIdentifier(node) {
  let current = node;
  while (current) {
    if (ts.isParameter(current) || ts.isPropertyDeclaration(current) || ts.isVariableDeclaration(current)) {
      if (current.name && ts.isIdentifier(current.name)) return current.name.text;
    }
    if (ts.isFunctionDeclaration(current) || ts.isMethodDeclaration(current) || ts.isArrowFunction(current)) {
      if (current.name && ts.isIdentifier(current.name)) return current.name.text;
    }
    current = current.parent;
  }
  return null;
}

function getParentName(node) {
  let current = node.parent;
  while (current) {
    if (ts.isPropertySignature(current) || ts.isPropertyDeclaration(current)) {
      if (ts.isIdentifier(current.name)) return current.name.text;
    }
    if (ts.isParameter(current) && ts.isIdentifier(current.name)) return current.name.text;
    if (ts.isVariableDeclaration(current) && ts.isIdentifier(current.name)) return current.name.text;
    current = current.parent;
  }
  return null;
}

function buildReplacementMap(sourceFile, filePath) {
  const relPath = path.relative(srcDir, filePath);
  const relDir = path.dirname(relPath);
  const map = new Map(); // start offset -> { replacement, imports }
  const neededImports = new Set();

  function addImport(typeName) {
    const src = importSource[typeName];
    if (src) neededImports.add({ typeName, src });
  }

  function replaceRange(start, end, text, typeName) {
    map.set(start, { start, end, text, typeName });
    if (typeName) addImport(typeName);
  }

  function visit(node) {
    if (node.kind === ts.SyntaxKind.AnyKeyword) {
      const parent = node.parent;
      const varName = findNodeIdentifier(parent) || getParentName(parent);
      const start = node.getStart(sourceFile);
      const end = node.getEnd();

      // Record<string, any>
      if (ts.isTypeReferenceNode(parent) && ts.isIdentifier(parent.typeName) && parent.typeName.text === "Record") {
        const childIdx = parent.typeArguments?.findIndex((t) => t === node);
        if (childIdx === 1) {
          replaceRange(start, end, "unknown");
          return;
        }
      }

      // Promise<any>
      if (ts.isTypeReferenceNode(parent) && ts.isIdentifier(parent.typeName) && parent.typeName.text === "Promise") {
        // known Promise<any> returns
        if (relPath.includes("detect_bootstrap/DetectBootstrapServiceHandler") && varName === "classify") {
          replaceRange(start, end, "ClassifyResponse", "ClassifyResponse");
        } else if (relPath.includes("ai_classifier") && varName === "callVertexAI") {
          replaceRange(start, end, "ClassifyResponse", "ClassifyResponse");
        } else {
          replaceRange(start, end, "unknown");
        }
        return;
      }

      // Array of any (any[])
      if (ts.isArrayTypeNode(parent) && parent.elementType === node) {
        replaceRange(start, end, "unknown");
        return;
      }

      // Index signatures
      if (ts.isIndexSignatureDeclaration(parent)) {
        replaceRange(start, end, "unknown");
        return;
      }

      // As expressions
      if (ts.isAsExpression(parent) && parent.type === node) {
        const exprText = parent.expression.getText(sourceFile);
        const left = exprText.trim();
        if (left === "counts") replaceRange(start, end, "JobCounts", "JobCounts");
        else if (left === "timings") replaceRange(start, end, "JobTimings", "JobTimings");
        else if (left === "part" && relPath.includes("stream_parser/ParquetWriterPool")) {
          replaceRange(start, end, "OutputPartCreationAttributes", "OutputPartCreationAttributes");
        } else if (left === "row" && relPath.includes("FinalizeRepository")) {
          replaceRange(start, end, "DeadLetterRow", "DeadLetterRow");
        } else if (left === "template" && relPath.includes("TemplateRegistry")) {
          replaceRange(start, end, "RecordTemplate | RubbishTemplate"); // no import, already imported in those files
        } else {
          replaceRange(start, end, "unknown");
        }
        return;
      }

      // Function return types
      if (ts.isFunctionDeclaration(parent) || ts.isMethodDeclaration(parent) || ts.isArrowFunction(parent)) {
        if (varName === "getLogger") replaceRange(start, end, "Logger", "Logger");
        else if (varName === "getFirestore") replaceRange(start, end, "Firestore", "Firestore");
        else if (varName === "getJob" && relPath.includes("FinalizeRepository")) replaceRange(start, end, "ParseJobRow | null", "ParseJobRow");
        else if (varName === "getJob" && relPath.includes("ReportServiceImpl")) replaceRange(start, end, "ParseJobRow | null", "ParseJobRow");
        else if (varName === "getParts" && relPath.includes("ReportServiceImpl")) replaceRange(start, end, "OutputPartRow[]", "OutputPartRow");
        else if (varName === "getBatchJobs" && relPath.includes("ReportServiceImpl")) replaceRange(start, end, "ParseJobRow[]", "ParseJobRow");
        else if (varName === "getCounts") replaceRange(start, end, "JobCounts", "JobCounts");
        else if (varName === "callVertexAI" && relPath.includes("ai_classifier")) replaceRange(start, end, "ClassifyResponse", "ClassifyResponse");
        else replaceRange(start, end, "unknown");
        return;
      }

      // Catch clause variable
      if (ts.isCatchClause(parent.parent) || ts.isVariableDeclaration(parent)) {
        const param = parent;
        if (ts.isCatchClause(param.parent) && param.name === node) {
          // node is the catch variable type? AnyKeyword can't be catch variable directly.
        }
      }

      // Variable/parameter/property names
      if (varName === "logger") replaceRange(start, end, "Logger", "Logger");
      else if (varName === "firestore") replaceRange(start, end, "Firestore", "Firestore");
      else if (varName === "req" || varName === "request") {
        if (relPath.includes("ai_classifier") || relPath.includes("detect_bootstrap") || relPath.includes("classifier")) {
          replaceRange(start, end, "ClassifyRequest", "ClassifyRequest");
        } else {
          replaceRange(start, end, "Request", "Request");
        }
      } else if (varName === "_res") replaceRange(start, end, "Response", "Response");
      else if (varName === "classify") replaceRange(start, end, "(req: ClassifyRequest) => Promise<ClassifyResponse>");
      else if (varName === "service") replaceRange(start, end, "ServiceManager", "ServiceManager");
      else if (varName === "details") replaceRange(start, end, "unknown");
      else if (varName === "value") replaceRange(start, end, "unknown");
      else if (varName === "raw") {
        if (relPath.includes("ai_classifier") || relPath.includes("detect_bootstrap")) replaceRange(start, end, "ClassifyResponse", "ClassifyResponse");
        else replaceRange(start, end, "Record<string, unknown>");
      } else if (varName === "field_spec") replaceRange(start, end, "string[]");
      else if (varName === "output_paths") replaceRange(start, end, "string[]");
      else if (varName === "counts") replaceRange(start, end, "JobCounts", "JobCounts");
      else if (varName === "timings") replaceRange(start, end, "JobTimings", "JobTimings");
      else if (varName === "fields") replaceRange(start, end, "Record<string, unknown>");
      else if (varName === "field_map") replaceRange(start, end, "Record<string, FieldLocator>", "FieldLocator");
      else if (varName === "data") {
        if (relPath.includes("IJobService") || relPath.includes("jobService")) replaceRange(start, end, "Record<string, unknown>");
        else if (parent && ts.isPropertySignature(parent) && parent.name && ts.isIdentifier(parent.name) && parent.name.text === "data") {
          replaceRange(start, end, "Record<string, unknown>");
        } else {
          replaceRange(start, end, "unknown");
        }
      } else if (varName === "err" || varName === "lastErr") replaceRange(start, end, "unknown");
      else if (varName === "message") replaceRange(start, end, "Record<string, unknown>");
      else if (varName === "params") {
        if (relPath.includes("CloudWatch")) replaceRange(start, end, "PutLogEventsCommandInput", "PutLogEventsCommandInput");
        else if (relPath.includes("QueueService")) replaceRange(start, end, "SendMessageCommandInput | ReceiveMessageCommandInput", "SendMessageCommandInput");
        else replaceRange(start, end, "Record<string, unknown>");
      } else if (varName === "cfg") replaceRange(start, end, "SQSClientConfig", "SQSClientConfig");
      else if (varName === "client") {
        if (relPath.includes("scripts")) replaceRange(start, end, "PoolClient", "PoolClient");
        else replaceRange(start, end, "unknown");
      } else if (varName === "m") {
        if (relPath.includes("QueueService")) replaceRange(start, end, "Message", "Message");
        else replaceRange(start, end, "unknown");
      } else if (varName === "arr") replaceRange(start, end, "unknown[]");
      else if (varName === "f") replaceRange(start, end, "unknown");
      else if (varName === "parsed") replaceRange(start, end, "unknown");
      else if (varName === "schemaObj") replaceRange(start, end, "Record<string, unknown>");
      else if (varName === "row") {
        if (relPath.includes("ParquetEngine")) replaceRange(start, end, "ParquetRow", "ParquetRow");
        else if (relPath.includes("TraceSystem")) replaceRange(start, end, "TraceRecord", "TraceRecord");
        else if (relPath.includes("DLQManager") && parent && ts.isTypeReferenceNode(parent) && parent.typeName && parent.typeName.text === "Promise") replaceRange(start, end, "DeadLetterEntry[]");
        else replaceRange(start, end, "unknown");
      } else if (varName === "fatal") replaceRange(start, end, "Error | null");
      else if (varName === "templateCache") replaceRange(start, end, "Map<string, Template>", "Template");
      else if (varName === "isRetryable") replaceRange(start, end, "(err: unknown) => boolean");
      else if (varName === "norm") replaceRange(start, end, "(field_spec: unknown) => string[]");
      else if (varName === "extractJson") replaceRange(start, end, "(text: string) => unknown");
      else if (varName === "value") replaceRange(start, end, "unknown");
      else replaceRange(start, end, "unknown");
    }
    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return { map, neededImports };
}

function processFile(filePath) {
  const sourceText = fs.readFileSync(filePath, "utf-8");
  const sourceFile = ts.createSourceFile(filePath, sourceText, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const { map, neededImports } = buildReplacementMap(sourceFile, filePath);
  if (map.size === 0) return { count: 0, neededImports };

  const replacements = [...map.values()].sort((a, b) => b.start - a.start);
  let newText = sourceText;
  for (const r of replacements) {
    newText = newText.substring(0, r.start) + r.text + newText.substring(r.end);
  }

  // Add imports for needed types
  if (neededImports.size > 0) {
    const existingImports = new Map();
    const importRegex = /import\s+(?:type\s+)?\{([^}]+)\}\s+from\s+"([^"]+)";/g;
    let m;
    while ((m = importRegex.exec(newText))) {
      existingImports.set(m[2], m[1]);
    }

    const bySource = new Map();
    for (const imp of neededImports) {
      if (imp.typeName === "ClassifyResponse" && imp.src === "shared/models/template.js" && newText.includes("ClassifyResponse")) {
        // already imported if existing
      }
      const rel = relativeImport(filePath, imp.src);
      if (!bySource.has(rel)) bySource.set(rel, new Set());
      bySource.get(rel).add(imp.typeName);
    }

    const importLines = [];
    for (const [src, names] of bySource) {
      const existing = existingImports.get(src);
      const existingNames = existing ? existing.split(",").map((s) => s.trim()) : [];
      const newNames = [...names].filter((n) => !existingNames.includes(n));
      if (newNames.length === 0) continue;
      const allNames = existing ? [...existingNames, ...newNames].join(", ") : newNames.join(", ");
      if (existing) {
        // replace existing import
        newText = newText.replace(`{${existing}} from "${src}";`, `{ ${allNames} } from "${src}";`);
      } else {
        importLines.push(`import { ${allNames} } from "${src}";`);
      }
    }
    if (importLines.length) {
      newText = importLines.join("\n") + "\n" + newText;
    }
  }

  fs.writeFileSync(filePath, newText, "utf-8");
  return { count: map.size };
}

function findTsFiles(dir) {
  const files = [];
  function walk(current) {
    if (current.includes("node_modules")) return;
    const stat = fs.statSync(current);
    if (stat.isDirectory()) {
      for (const child of fs.readdirSync(current)) walk(path.join(current, child));
    } else if ((current.endsWith(".ts") || current.endsWith(".tsx")) && !current.endsWith(".d.ts")) {
      files.push(current);
    }
  }
  walk(dir);
  return files;
}

const files = findTsFiles(srcDir);
let total = 0;
for (const file of files) {
  const res = processFile(file);
  if (res.count > 0) {
    console.log(`${file}: replaced ${res.count} any(s)`);
    total += res.count;
  }
}
console.log(`Total replaced: ${total}`);
