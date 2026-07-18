import fs from "fs";
import path from "path";
import ts from "typescript";

const SRC_DIR = "src";
const IGNORE_PATTERN = /\/\/\s*(TODO|FIXME|eslint)/i;

function collectCommentRanges(sourceFile, text) {
    const ranges = [];
    function visit(node) {
        const leading = ts.getLeadingCommentRanges(text, node.getFullStart());
        if (leading) ranges.push(...leading);
        const trailing = ts.getTrailingCommentRanges(text, node.getEnd());
        if (trailing) ranges.push(...trailing);
        ts.forEachChild(node, visit);
    }
    visit(sourceFile);
    return ranges;
}

function removeComment(text, range) {
    // Remove only single-line // comments
    if (range.kind !== ts.SyntaxKind.SingleLineCommentTrivia) return text;
    const commentText = text.slice(range.pos, range.end);
    if (IGNORE_PATTERN.test(commentText)) return text;

    let start = range.pos;
    let end = range.end;

    // Expand to remove end-of-line (\r\n or \n)
    let eol = text.indexOf("\n", end);
    if (eol === -1) {
        eol = text.length;
    }

    // Determine if this is a standalone comment line or a trailing inline comment
    const lineStart = text.lastIndexOf("\n", start - 1) + 1;
    const before = text.slice(lineStart, start);
    const beforeTrimmed = before.trim();

    if (beforeTrimmed === "") {
        // Standalone comment line: remove the whole line including its newline
        start = lineStart;
        end = eol + 1;
    } else {
        // Trailing inline comment: remove the comment and preceding whitespace on the same line,
        // but keep the newline. Also consume a preceding \r if present.
        let codeEnd = start - 1;
        while (codeEnd >= 0 && (text[codeEnd] === " " || text[codeEnd] === "\t")) codeEnd--;
        if (codeEnd >= 0 && text[codeEnd] === "\r") codeEnd--;
        start = codeEnd + 1;
        end = eol; // keep the newline
    }

    return text.slice(0, start) + text.slice(end);
}

function processFile(filePath) {
    const text = fs.readFileSync(filePath, "utf8");
    const sourceFile = ts.createSourceFile(filePath, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    let ranges = collectCommentRanges(sourceFile, text);
    // Deduplicate and sort descending so removals don't shift positions
    const seen = new Set();
    ranges = ranges.filter((r) => {
        const key = `${r.pos}-${r.end}`;
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
    });
    ranges.sort((a, b) => b.pos - a.pos);

    let newText = text;
    for (const range of ranges) {
        newText = removeComment(newText, range);
    }
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
console.log("Comment cleanup complete.");
