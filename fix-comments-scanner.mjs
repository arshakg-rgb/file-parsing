import fs from "fs";
import path from "path";
import ts from "typescript";

const SRC_DIR = "src";
const IGNORE_PATTERN = /\/\/\s*(TODO|FIXME|eslint)/i;

function removeSingleLineComments(text) {
    const scanner = ts.createScanner(ts.ScriptTarget.Latest, false, ts.LanguageVariant.Standard, text);
    const comments = [];
    while (true) {
        const token = scanner.scan();
        if (token === ts.SyntaxKind.EndOfFileToken) break;
        if (token === ts.SyntaxKind.SingleLineCommentTrivia) {
            comments.push({ pos: scanner.getTokenPos(), end: scanner.getTextPos(), text: scanner.getTokenText() });
        }
    }

    // Sort descending by position so removals don't shift
    comments.sort((a, b) => b.pos - a.pos);

    for (const range of comments) {
        if (IGNORE_PATTERN.test(range.text)) continue;

        let start = range.pos;
        let end = range.end;

        const lineStart = text.lastIndexOf("\n", start - 1) + 1;
        const eol = text.indexOf("\n", end);
        const eolIdx = eol === -1 ? text.length : eol;
        const before = text.slice(lineStart, start);

        if (before.trim() === "") {
            // Standalone comment line
            start = lineStart;
            end = eolIdx + 1;
        } else {
            // Inline trailing comment – remove it and preceding whitespace, keep newline
            let codeEnd = start - 1;
            while (codeEnd >= 0 && (text[codeEnd] === " " || text[codeEnd] === "\t")) codeEnd--;
            if (codeEnd >= 0 && text[codeEnd] === "\r") codeEnd--;
            start = codeEnd + 1;
            end = eolIdx;
        }

        text = text.slice(0, start) + text.slice(end);
    }

    return text;
}

function processFile(filePath) {
    const text = fs.readFileSync(filePath, "utf8");
    const newText = removeSingleLineComments(text);
    if (newText !== text) {
        fs.writeFileSync(filePath, newText);
        console.log(`Cleaned comments in ${filePath}`);
    }
}

function walk(dir) {
    for (const entry of fs.readdirSync(dir)) {
        const full = path.join(dir, entry);
        const stat = fs.statSync(full);
        if (stat.isDirectory()) {
            walk(full);
        } else if (full.endsWith(".ts") && !full.endsWith(".d.ts")) {
            processFile(full);
        }
    }
}

walk(SRC_DIR);
console.log("Scanner comment cleanup complete.");
