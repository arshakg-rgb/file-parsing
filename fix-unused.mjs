import { Project, SyntaxKind } from "ts-morph";
import { readFileSync, writeFileSync } from "fs";
import { execSync } from "child_process";

// Generate fresh ESLint JSON report (lint may exit 1 because of errors)
try {
    execSync("npx eslint src --format json -o /tmp/eslint-report.json", { stdio: "pipe" });
} catch {
    // ignore
}
const report = JSON.parse(readFileSync("/tmp/eslint-report.json", "utf8"));

const project = new Project({ tsConfigFilePath: "tsconfig.json" });
const files = new Set();
report.forEach((f) => f.messages.forEach((m) => {
    if (m.ruleId.includes("no-unused-vars")) files.add(f.filePath);
}));
files.forEach((f) => project.addSourceFileAtPath(f));

function getPosition(sf, line, column) {
    const text = sf.getFullText();
    const lines = text.split("\n");
    let pos = 0;
    for (let i = 0; i < line - 1; i++) pos += lines[i].length + 1;
    pos += column - 1;
    return pos;
}

function extractName(message) {
    const m = message.match(/'([^']+)'/);
    return m ? m[1] : null;
}

function hasSideEffects(node) {
    if (!node) return false;
    const kind = node.getKind();
    const sideEffectKinds = [
        SyntaxKind.CallExpression,
        SyntaxKind.NewExpression,
        SyntaxKind.AwaitExpression,
        SyntaxKind.TaggedTemplateExpression,
        SyntaxKind.DeleteExpression,
        SyntaxKind.PostfixUnaryExpression,
        SyntaxKind.YieldExpression,
        SyntaxKind.AssignmentExpression,
    ];
    if (sideEffectKinds.includes(kind)) return true;
    // Prefix ++, --, delete have side effects; !, +, -, ~ do not.
    if (kind === SyntaxKind.PrefixUnaryExpression) {
        const op = node.getOperatorToken().getKind();
        if (op === SyntaxKind.PlusPlusToken || op === SyntaxKind.MinusMinusToken || op === SyntaxKind.DeleteKeyword) {
            return true;
        }
    }
    for (const child of node.getChildren()) {
        if (hasSideEffects(child)) return true;
    }
    return false;
}

report.forEach((f) => {
    const sf = project.getSourceFile(f.filePath);
    if (!sf) return;
    const msgs = f.messages.filter((m) => m.severity === 2 && m.ruleId.includes("no-unused-vars"));
    // Process from bottom to top to keep positions valid
    msgs.sort((a, b) => b.line - a.line || b.column - a.column);
    msgs.forEach((m) => {
        const name = extractName(m.message);
        if (!name) return;
        const pos = getPosition(sf, m.line, m.column);
        let node = sf.getDescendantAtPos(pos);
        if (!node) return;
        // Climb to the relevant named declaration
        while (node && !node.getKindName().match(/^(ParameterDeclaration|VariableDeclaration|BindingElement)$/)) {
            node = node.getParent();
            if (!node || node.getKindName() === "SourceFile") break;
        }
        if (!node) return;
        if (node.getKindName() === "ParameterDeclaration" || node.getKindName() === "BindingElement") {
            try {
                node.rename(`_${name}`);
            } catch (e) {
                // Fallback: rename the identifier directly
                const id = sf.getDescendantAtPos(pos);
                if (id && id.getKind() === SyntaxKind.Identifier) id.rename(`_${name}`);
            }
            return;
        }
        if (node.getKindName() === "VariableDeclaration") {
            const initializer = node.getInitializer();
            if (initializer && hasSideEffects(initializer)) {
                try {
                    node.rename(`_${name}`);
                } catch {
                    const id = node.getNameNode();
                    if (id && id.getKind() === SyntaxKind.Identifier) id.rename(`_${name}`);
                }
            } else {
                try {
                    node.remove();
                } catch {
                    // leave as underscore if cannot remove
                    try {
                        node.rename(`_${name}`);
                    } catch {}
                }
            }
        }
    });
});

await project.save();
console.log("Fixed no-unused-vars warnings.");
