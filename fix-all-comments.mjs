import fs from "fs";
import path from "path";
import ts from "typescript";

const SRC_DIR = "src";
const IGNORE_PATTERN = new RegExp("^//\\s*(TODO|FIXME|eslint)", "i");

function removeSingleLineComments(text) {
    const sourceFile = ts.createSourceFile("", text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
    const ranges = [];

    function visit(node) {
        const leading = ts.getLeadingCommentRanges(text, node.getFullStart());
        if (leading) ranges.push(...leading);
        const trailing = ts.getTrailingCommentRanges(text, node.getEnd());
        if (trailing) ranges.push(...trailing);
        ts.forEachChild(node, visit);
    }
    visit(sourceFile);

    const seen = new Set();
    const unique = ranges
        .filter((r) => {
            const key = `${r.pos}-${r.end}`;
            if (seen.has(key)) return false;
            seen.add(key);
            return r.kind === ts.SyntaxKind.SingleLineCommentTrivia;
        })
        .sort((a, b) => b.pos - a.pos);

    for (const range of unique) {
        const commentText = text.slice(range.pos, range.end);
        if (IGNORE_PATTERN.test(commentText)) continue;

        // Determine if the comment is inline (same line as code before it) or standalone
        const lineStart = text.lastIndexOf("\n", range.pos - 1) + 1;
        const eol = text.indexOf("\n", range.end);
        const eolIdx = eol === -1 ? text.length : eol;
        const before = text.slice(lineStart, range.pos);

        let start, end;
        if (before.trim() === "") {
            // Standalone comment line – remove the whole line
            start = lineStart;
            end = eolIdx + 1; // include newline
        } else {
            // Inline comment after code – remove the comment and preceding whitespace,
            // keep the newline. Also consume a preceding \r if present.
            let codeEnd = range.pos - 1;
            while (codeEnd >= 0 && (text[codeEnd] === " " || text[codeEnd] === "\t")) codeEnd--;
            if (codeEnd >= 0 && text[codeEnd] === "\r") codeEnd--;
            start = codeEnd + 1;
            end = eolIdx; // keep the newline
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
console.log("All single-line comment cleanup complete.");
