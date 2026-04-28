#!/usr/bin/env tsx
/**
 * Migration script: GitBook v3.3 → Docusaurus 3
 * Reads from docs/gitbook/src/ and writes to docs/docusaurus/docs/
 */

import * as fs from 'fs';
import * as path from 'path';

const GITBOOK_SRC = '/home/bloxster/erigon/docs/gitbook/src';
const DOCUSAURUS_DOCS = '/home/bloxster/erigon/docs/docusaurus/docs';
const GITBOOK_INCLUDES = path.join(GITBOOK_SRC, '.gitbook/includes');
const GITBOOK_ASSETS = path.join(GITBOOK_SRC, '.gitbook/assets');
const GITBOOK_IMAGES = path.join(GITBOOK_SRC, 'images');
const STATIC_IMG = '/home/bloxster/erigon/docs/docusaurus/static/img';

// File mapping: source (relative to GITBOOK_SRC) -> target (relative to DOCUSAURUS_DOCS)
const FILE_MAP: Record<string, string | null> = {
  'README.md': null, // skip - will be created manually
  'get-started/readme/why-using-erigon.md': 'get-started/why-using-erigon.md',
  'get-started/hardware-requirements.md': 'get-started/hardware-requirements.md',
  'get-started/installation-2/README.md': 'get-started/installation/index.md',
  'get-started/installation-2/upgrading.md': 'get-started/installation/upgrading.md',
  'get-started/migrating-from-geth.md': 'get-started/migrating-from-geth.md',
  'get-started/easy-nodes/README.md': 'get-started/easy-nodes/index.md',
  'get-started/easy-nodes/how-to-run-an-ethereum-node/README.md': 'get-started/easy-nodes/how-to-run-an-ethereum-node/index.md',
  'get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl.md': 'get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl.md',
  'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/README.md': 'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/index.md',
  'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl.md': 'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl.md',
  'get-started/easy-nodes/how-to-run-a-polygon-node.md': 'get-started/easy-nodes/how-to-run-a-polygon-node.md',
  'get-started/fundamentals/basic-usage.md': 'fundamentals/basic-usage.md',
  'get-started/fundamentals/sync-modes.md': 'fundamentals/sync-modes.md',
  'get-started/fundamentals/configuring-erigon.md': 'fundamentals/configuring-erigon/index.md',
  'get-started/fundamentals/supported-networks.md': 'fundamentals/supported-networks.md',
  'get-started/fundamentals/default-ports.md': 'fundamentals/default-ports.md',
  'get-started/fundamentals/layer-2-networks.md': 'fundamentals/layer-2-networks.md',
  'get-started/fundamentals/caplin.md': 'fundamentals/caplin.md',
  'get-started/fundamentals/optimizing-storage.md': 'fundamentals/optimizing-storage.md',
  'get-started/fundamentals/performance-tricks.md': 'fundamentals/performance-tricks.md',
  'get-started/fundamentals/logs.md': 'fundamentals/logs.md',
  'get-started/fundamentals/security.md': 'fundamentals/security.md',
  'get-started/fundamentals/jwt.md': 'fundamentals/jwt.md',
  'get-started/fundamentals/tls-authentication.md': 'fundamentals/tls-authentication.md',
  'get-started/fundamentals/multiple-instances.md': 'fundamentals/multiple-instances.md',
  'get-started/fundamentals/modules/README.md': 'fundamentals/modules/index.md',
  'get-started/fundamentals/modules/rpc-daemon.md': 'fundamentals/modules/rpc-daemon.md',
  'get-started/fundamentals/modules/txpool.md': 'fundamentals/modules/txpool.md',
  'get-started/fundamentals/modules/sentry.md': 'fundamentals/modules/sentry.md',
  'get-started/fundamentals/modules/downloader.md': 'fundamentals/modules/downloader.md',
  'get-started/fundamentals/interacting-with-erigon/README.md': 'interacting-with-erigon/index.md',
  'get-started/fundamentals/interacting-with-erigon/eth.md': 'interacting-with-erigon/eth.md',
  'get-started/fundamentals/interacting-with-erigon/erigon.md': 'interacting-with-erigon/erigon.md',
  'get-started/fundamentals/interacting-with-erigon/engine.md': 'interacting-with-erigon/engine.md',
  'get-started/fundamentals/interacting-with-erigon/web3.md': 'interacting-with-erigon/web3.md',
  'get-started/fundamentals/interacting-with-erigon/net.md': 'interacting-with-erigon/net.md',
  'get-started/fundamentals/interacting-with-erigon/debug.md': 'interacting-with-erigon/debug.md',
  'get-started/fundamentals/interacting-with-erigon/trace.md': 'interacting-with-erigon/trace.md',
  'get-started/fundamentals/interacting-with-erigon/txpool.md': 'interacting-with-erigon/txpool.md',
  'get-started/fundamentals/interacting-with-erigon/admin.md': 'interacting-with-erigon/admin.md',
  'get-started/fundamentals/interacting-with-erigon/bor.md': 'interacting-with-erigon/bor.md',
  'get-started/fundamentals/interacting-with-erigon/ots.md': 'interacting-with-erigon/ots.md',
  'get-started/fundamentals/interacting-with-erigon/internal.md': 'interacting-with-erigon/internal.md',
  'get-started/fundamentals/interacting-with-erigon/grpc.md': 'interacting-with-erigon/grpc.md',
  'get-started/fundamentals/creating-a-dashboard.md': 'fundamentals/creating-a-dashboard.md',
  'get-started/fundamentals/docker-compose.md': 'fundamentals/docker-compose.md',
  'get-started/fundamentals/otterscan.md': 'fundamentals/otterscan.md',
  'get-started/fundamentals/web3-wallet.md': 'fundamentals/web3-wallet.md',
  'get-started/fundamentals/staking/README.md': 'staking/index.md',
  'get-started/fundamentals/staking/caplin.md': 'staking/caplin.md',
  'get-started/fundamentals/staking/external-consensus-client-as-validator.md': 'staking/external-consensus-client-as-validator.md',
  'get-started/fundamentals/staking/shutter-network.md': 'staking/shutter-network.md',
  'get-started/about/README.md': 'about/index.md',
  'get-started/about/contributing.md': 'about/contributing.md',
  'get-started/about/license.md': 'about/license.md',
  'get-started/about/disclaimer.md': 'about/disclaimer.md',
  'get-started/about/donate.md': 'about/donate.md',
  'get-started/about/how-to-reach-us.md': 'about/how-to-reach-us.md',
};

// Sidebar positions based on SUMMARY.md order
// Format: target path -> sidebar_position
const SIDEBAR_POSITIONS: Record<string, number> = {
  'get-started/why-using-erigon.md': 2,
  'get-started/hardware-requirements.md': 2,
  'get-started/installation/index.md': 3,
  'get-started/installation/upgrading.md': 1,
  'get-started/migrating-from-geth.md': 4,
  'get-started/easy-nodes/index.md': 1,
  'get-started/easy-nodes/how-to-run-an-ethereum-node/index.md': 1,
  'get-started/easy-nodes/how-to-run-an-ethereum-node/ethereum-with-an-external-cl.md': 1,
  'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/index.md': 1,
  'get-started/easy-nodes/how-to-run-a-gnosis-chain-node/gnosis-with-an-external-cl.md': 1,
  'get-started/easy-nodes/how-to-run-a-polygon-node.md': 3,
  'fundamentals/basic-usage.md': 1,
  'fundamentals/sync-modes.md': 2,
  'fundamentals/configuring-erigon/index.md': 3,
  'fundamentals/supported-networks.md': 4,
  'fundamentals/default-ports.md': 5,
  'fundamentals/layer-2-networks.md': 6,
  'fundamentals/caplin.md': 7,
  'fundamentals/optimizing-storage.md': 8,
  'fundamentals/performance-tricks.md': 9,
  'fundamentals/logs.md': 10,
  'fundamentals/security.md': 11,
  'fundamentals/jwt.md': 12,
  'fundamentals/tls-authentication.md': 13,
  'fundamentals/multiple-instances.md': 14,
  'fundamentals/modules/index.md': 1,
  'fundamentals/modules/rpc-daemon.md': 1,
  'fundamentals/modules/txpool.md': 2,
  'fundamentals/modules/sentry.md': 3,
  'fundamentals/modules/downloader.md': 4,
  'interacting-with-erigon/index.md': 1,
  'interacting-with-erigon/eth.md': 1,
  'interacting-with-erigon/erigon.md': 2,
  'interacting-with-erigon/engine.md': 3,
  'interacting-with-erigon/web3.md': 4,
  'interacting-with-erigon/net.md': 5,
  'interacting-with-erigon/debug.md': 6,
  'interacting-with-erigon/trace.md': 7,
  'interacting-with-erigon/txpool.md': 8,
  'interacting-with-erigon/admin.md': 9,
  'interacting-with-erigon/bor.md': 10,
  'interacting-with-erigon/ots.md': 11,
  'interacting-with-erigon/internal.md': 12,
  'interacting-with-erigon/grpc.md': 13,
  'fundamentals/creating-a-dashboard.md': 16,
  'fundamentals/docker-compose.md': 17,
  'fundamentals/otterscan.md': 18,
  'fundamentals/web3-wallet.md': 19,
  'staking/index.md': 1,
  'staking/caplin.md': 1,
  'staking/external-consensus-client-as-validator.md': 2,
  'staking/shutter-network.md': 3,
  'about/index.md': 1,
  'about/contributing.md': 1,
  'about/license.md': 2,
  'about/disclaimer.md': 3,
  'about/donate.md': 4,
  'about/how-to-reach-us.md': 5,
};

// Category JSON files to create
const CATEGORIES: Record<string, { label: string; position: number }> = {
  'get-started': { label: 'Get Started', position: 1 },
  'get-started/installation': { label: 'Installation', position: 3 },
  'get-started/easy-nodes': { label: 'Easy Nodes', position: 4 },
  'get-started/easy-nodes/how-to-run-an-ethereum-node': { label: 'How to Run an Ethereum Node', position: 1 },
  'get-started/easy-nodes/how-to-run-a-gnosis-chain-node': { label: 'How to Run a Gnosis Chain Node', position: 2 },
  'fundamentals': { label: 'Fundamentals', position: 2 },
  'fundamentals/modules': { label: 'Modules', position: 9 },
  'fundamentals/configuring-erigon': { label: 'CLI Reference', position: 3 },
  'interacting-with-erigon': { label: 'Interacting with Erigon', position: 3 },
  'staking': { label: 'Staking', position: 4 },
  'about': { label: 'About', position: 5 },
};

function loadIncludeFile(includePath: string, sourceFile: string): string {
  // includePath is relative to the source file, e.g. "../../.gitbook/includes/foo.md"
  // or it might already contain .gitbook
  let resolvedPath: string;
  if (includePath.includes('.gitbook/includes/')) {
    // Extract just the filename part
    const includesDir = path.join(GITBOOK_INCLUDES);
    const filename = includePath.split('.gitbook/includes/')[1];
    resolvedPath = path.join(includesDir, filename);
  } else {
    resolvedPath = path.resolve(path.dirname(sourceFile), includePath);
  }

  try {
    let content = fs.readFileSync(resolvedPath, 'utf-8');
    // Strip frontmatter from included file
    content = content.replace(/^---\n[\s\S]*?\n---\n/, '');
    return content.trim();
  } catch (e) {
    console.warn(`  [WARN] Could not read include: ${resolvedPath}`);
    return `<!-- include not found: ${includePath} -->`;
  }
}

function transformContent(content: string, sourceFile: string, targetPath: string): string {
  // a) Inline GitBook includes
  content = content.replace(
    /\{%\s*include\s+"([^"]+)"\s*%\}/g,
    (_match, includePath) => loadIncludeFile(includePath, sourceFile)
  );

  // b) Remove frontmatter description field (including multi-line block scalar values)
  content = content.replace(/^(---\n)([\s\S]*?)(---\n)/, (_match, open, body, close) => {
    // Remove description field: handles both single-line and multi-line (>-, |, >) block scalar values
    // A field spans from "description:" until the next key (line not starting with space/tab) or end of frontmatter
    let cleanedBody = body.replace(/^description:.*\n(?:[ \t]+.*\n)*/m, '');
    return open + cleanedBody + close;
  });

  // c) Hints -> admonitions
  content = content.replace(/\{%\s*hint\s+style="info"\s*%\}/g, ':::note');
  content = content.replace(/\{%\s*hint\s+style="success"\s*%\}/g, ':::tip');
  content = content.replace(/\{%\s*hint\s+style="warning"\s*%\}/g, ':::warning');
  content = content.replace(/\{%\s*hint\s+style="danger"\s*%\}/g, ':::danger');
  content = content.replace(/\{%\s*endhint\s*%\}/g, ':::');

  // d) Tabs -> MDX Tabs
  const hasTabs = /\{%\s*tabs\s*%\}/.test(content);
  if (hasTabs) {
    // Add import after frontmatter (or at top if no frontmatter)
    const tabsImport = "import Tabs from '@theme/Tabs';\nimport TabItem from '@theme/TabItem';\n\n";
    if (/^---\n[\s\S]*?\n---\n/.test(content)) {
      content = content.replace(/^(---\n[\s\S]*?\n---\n)/, `$1\n${tabsImport}`);
    } else {
      content = tabsImport + content;
    }

    content = content.replace(/\{%\s*tabs\s*%\}/g, '<Tabs>');
    content = content.replace(/\{%\s*tab\s+title="([^"]+)"\s*%\}/g, (_match, title) => {
      const value = title.replace(/\s+/g, '-').toLowerCase();
      return `<TabItem value="${value}" label="${title}">`;
    });
    content = content.replace(/\{%\s*endtab\s*%\}/g, '</TabItem>');
    content = content.replace(/\{%\s*endtabs\s*%\}/g, '</Tabs>');
  }

  // e) Remove code block wrappers
  content = content.replace(/\{%\s*code[^%]*%\}\n?/g, '');
  content = content.replace(/\{%\s*endcode\s*%\}\n?/g, '');

  // f) Remove content-ref blocks
  content = content.replace(/\{%\s*content-ref[^%]*%\}[\s\S]*?\{%\s*endcontent-ref\s*%\}/g, '');

  // g) Image paths
  // HTML src attributes
  content = content.replace(/src="\.\.\/\.\.\/\.gitbook\/assets\//g, 'src="/img/');
  content = content.replace(/src="\.\.\/\.gitbook\/assets\//g, 'src="/img/');
  content = content.replace(/src="\.\.\/\.\.\/\.\.\/\.gitbook\/assets\//g, 'src="/img/');
  content = content.replace(/src="\.gitbook\/assets\//g, 'src="/img/');
  // Markdown image syntax
  content = content.replace(/!\[([^\]]*)\]\(\.\.\/\.\.\/\.gitbook\/assets\//g, '![$1](/img/');
  content = content.replace(/!\[([^\]]*)\]\(\.\.\/\.gitbook\/assets\//g, '![$1](/img/');
  content = content.replace(/!\[([^\]]*)\]\(\.\.\/\.\.\/\.\.\/\.gitbook\/assets\//g, '![$1](/img/');
  content = content.replace(/!\[([^\]]*)\]\(\.gitbook\/assets\//g, '![$1](/img/');
  // images/ directory references
  content = content.replace(/!\[([^\]]*)\]\(\.\.\/\.\.\/images\//g, '![$1](/img/');
  content = content.replace(/!\[([^\]]*)\]\(\.\.\/images\//g, '![$1](/img/');
  content = content.replace(/!\[([^\]]*)\]\(images\//g, '![$1](/img/');

  // h) GitBook expression variables
  content = content.replace(/<code class="expression">space\.vars\.version<\/code>/g, 'v3.3.10');

  // i) Relative link updates - strip .md extensions from links, fix README -> index
  // Fix README.md links -> index.md or /
  content = content.replace(/\(([^)]*?)\/README\.md\)/g, '($1/)');
  content = content.replace(/\(README\.md\)/g, '(/)');
  // Strip .md extensions from markdown links
  content = content.replace(/\(([^)#]+?)\.md(#[^)]*)?\)/g, (_match, linkPath, anchor) => {
    return `(${linkPath}${anchor || ''})`;
  });

  // j) Remove <figure> wrappers
  content = content.replace(
    /<figure>\s*<img\s+src="([^"]+)"[^>]*>\s*<figcaption[^>]*>([\s\S]*?)<\/figcaption>\s*<\/figure>/g,
    (_match, src, caption) => {
      const cleanCaption = caption.replace(/<[^>]+>/g, '').trim();
      return `![${cleanCaption}](${src})`;
    }
  );
  // Handle figures without figcaption
  content = content.replace(
    /<figure>\s*<img\s+src="([^"]+)"(?:\s+alt="([^"]*)")?\s*\/?>\s*<\/figure>/g,
    (_match, src, alt) => `![${alt || ''}](${src})`
  );
  // Handle figures with alt but no figcaption text
  content = content.replace(
    /<figure>\s*<img\s+src="([^"]+)"[^>]*>\s*<figcaption>\s*<\/figcaption>\s*<\/figure>/g,
    (_match, src) => `![](${src})`
  );

  // k) Remove card table HTML blocks
  content = content.replace(/<table\s+data-view="cards"[\s\S]*?<\/table>/g, '');

  // l0) Fix self-closing HTML tags for MDX compatibility
  // Convert <br> to <br/> (MDX requires self-closing)
  content = content.replace(/<br>/g, '<br/>');
  // Convert markdown autolinks <https://...> to standard links [url](url) for MDX compatibility
  content = content.replace(/<(https?:\/\/[^>]+)>/g, '[$1]($1)');
  // Remove $$ ... $$ math blocks (GitBook math inline rendering - strip to plain text)
  // Pattern: $$ latex_expr $$ - extract any \text{} content within, or remove entirely
  content = content.replace(/\$\$\s*\$\\text\{([^}]+)\}\$\s*\$\$/g, (_match, textContent) => {
    // Unescape underscores in text content
    return textContent.replace(/\\_/g, '_');
  });
  // Remove any remaining $$ ... $$ blocks
  content = content.replace(/\$\$[^$]*\$\$/g, '');

  // l) Convert {% embed url="..." %} to markdown links
  content = content.replace(/\{%\s*embed\s+url="([^"]+)"\s*%\}/g, (_match, url) => {
    return `[${url}](${url})`;
  });

  // m) Convert stepper/step blocks to plain markdown
  content = content.replace(/\{%\s*stepper\s*%\}/g, '');
  content = content.replace(/\{%\s*endstepper\s*%\}/g, '');
  content = content.replace(/\{%\s*step\s*%\}/g, '');
  content = content.replace(/\{%\s*endstep\s*%\}/g, '');

  // n) Remove escaped GitBook tags left as \{%...\} (from source that escaped them)
  // The actual bytes are backslash + {%...%backslash+}
  // Also handle cases where escaped tag is followed immediately by more content on same line
  content = content.replace(/\\\{%[^%]*%\\\}\s*/g, '\n\n');

  // Clean up multiple blank lines left by removals
  content = content.replace(/\n{3,}/g, '\n\n');

  return content;
}

function addSidebarPosition(content: string, targetRelPath: string): string {
  const position = SIDEBAR_POSITIONS[targetRelPath];
  if (position === undefined) return content;

  // Check if there's existing frontmatter
  if (/^---\n/.test(content)) {
    // Add sidebar_position to existing frontmatter
    content = content.replace(/^---\n/, `---\nsidebar_position: ${position}\n`);
  } else {
    // Add frontmatter with sidebar_position
    content = `---\nsidebar_position: ${position}\n---\n\n` + content;
  }
  return content;
}

function ensureDir(dirPath: string): void {
  fs.mkdirSync(dirPath, { recursive: true });
}

function copyImages(): void {
  console.log('\n=== Copying images ===');
  ensureDir(STATIC_IMG);

  // Copy from .gitbook/assets/
  if (fs.existsSync(GITBOOK_ASSETS)) {
    const files = fs.readdirSync(GITBOOK_ASSETS);
    for (const file of files) {
      const src = path.join(GITBOOK_ASSETS, file);
      const dest = path.join(STATIC_IMG, file);
      if (fs.statSync(src).isFile()) {
        fs.copyFileSync(src, dest);
        console.log(`  Copied: ${file}`);
      }
    }
  }

  // Copy from images/
  if (fs.existsSync(GITBOOK_IMAGES)) {
    const files = fs.readdirSync(GITBOOK_IMAGES);
    for (const file of files) {
      const src = path.join(GITBOOK_IMAGES, file);
      const dest = path.join(STATIC_IMG, file);
      if (fs.statSync(src).isFile()) {
        fs.copyFileSync(src, dest);
        console.log(`  Copied (images/): ${file}`);
      }
    }
  }
}

function createCategoryFiles(): void {
  console.log('\n=== Creating _category_.json files ===');
  for (const [dirRelPath, meta] of Object.entries(CATEGORIES)) {
    const dirPath = path.join(DOCUSAURUS_DOCS, dirRelPath);
    ensureDir(dirPath);
    const categoryFile = path.join(dirPath, '_category_.json');
    fs.writeFileSync(categoryFile, JSON.stringify(meta, null, 2) + '\n');
    console.log(`  Created: ${dirRelPath}/_category_.json`);
  }
}

function createIndexMdx(): void {
  console.log('\n=== Creating docs/index.mdx ===');
  const indexContent = `---
sidebar_position: 1
slug: /
---

# Erigon Documentation

Welcome to the Erigon v3.3 documentation.

Erigon is an implementation of Ethereum (execution layer with embeddable consensus layer), on the efficiency frontier.

import Link from '@docusaurus/Link';

<div className="lp-grid">
  <Link to="/get-started/" className="lp-card">
    <div className="lp-card-title">Get Started</div>
    <div className="lp-card-desc">Installation, hardware requirements, easy nodes</div>
  </Link>
  <Link to="/fundamentals/" className="lp-card">
    <div className="lp-card-title">Fundamentals</div>
    <div className="lp-card-desc">Configuration, sync modes, modules, and more</div>
  </Link>
  <Link to="/interacting-with-erigon/" className="lp-card">
    <div className="lp-card-title">Interacting with Erigon</div>
    <div className="lp-card-desc">JSON-RPC API reference</div>
  </Link>
  <Link to="/staking/" className="lp-card">
    <div className="lp-card-title">Staking</div>
    <div className="lp-card-desc">Caplin validator, external CL, Shutter Network</div>
  </Link>
</div>
`;
  ensureDir(DOCUSAURUS_DOCS);
  fs.writeFileSync(path.join(DOCUSAURUS_DOCS, 'index.mdx'), indexContent);
  console.log('  Created: docs/index.mdx');
}

function migrateFiles(): void {
  console.log('\n=== Migrating files ===');
  const created: string[] = [];
  const skipped: string[] = [];
  const errors: string[] = [];

  for (const [srcRelPath, targetRelPath] of Object.entries(FILE_MAP)) {
    if (targetRelPath === null) {
      console.log(`  SKIP: ${srcRelPath} (will be created manually)`);
      skipped.push(srcRelPath);
      continue;
    }

    const srcFile = path.join(GITBOOK_SRC, srcRelPath);
    const targetFile = path.join(DOCUSAURUS_DOCS, targetRelPath);

    if (!fs.existsSync(srcFile)) {
      console.warn(`  [WARN] Source not found: ${srcFile}`);
      errors.push(srcRelPath);
      continue;
    }

    try {
      let content = fs.readFileSync(srcFile, 'utf-8');
      content = transformContent(content, srcFile, targetRelPath);
      content = addSidebarPosition(content, targetRelPath);

      ensureDir(path.dirname(targetFile));
      fs.writeFileSync(targetFile, content);
      console.log(`  OK: ${srcRelPath} -> ${targetRelPath}`);
      created.push(targetRelPath);
    } catch (e) {
      console.error(`  [ERROR] Failed to migrate ${srcRelPath}: ${e}`);
      errors.push(srcRelPath);
    }
  }

  console.log(`\nMigrated: ${created.length} files`);
  console.log(`Skipped: ${skipped.length} files`);
  console.log(`Errors: ${errors.length} files`);
}

function checkRemainingGitBookTags(): void {
  console.log('\n=== Checking for remaining GitBook tags ===');
  const files = fs.readdirSync(DOCUSAURUS_DOCS, { recursive: true }) as string[];
  let found = false;
  for (const file of files) {
    if (!file.endsWith('.md') && !file.endsWith('.mdx')) continue;
    const fullPath = path.join(DOCUSAURUS_DOCS, file);
    if (!fs.statSync(fullPath).isFile()) continue;
    const content = fs.readFileSync(fullPath, 'utf-8');
    if (/{%/.test(content)) {
      console.warn(`  [WARN] Remaining GitBook tags in: ${file}`);
      const lines = content.split('\n');
      lines.forEach((line, i) => {
        if (/{%/.test(line)) {
          console.warn(`    Line ${i + 1}: ${line.trim()}`);
        }
      });
      found = true;
    }
  }
  if (!found) {
    console.log('  No remaining GitBook tags found.');
  }
}

// Main execution
async function main() {
  console.log('=== Erigon v3.3 GitBook -> Docusaurus Migration ===\n');

  copyImages();
  createCategoryFiles();
  migrateFiles();
  createIndexMdx();
  checkRemainingGitBookTags();

  console.log('\n=== Migration complete ===');
}

main().catch(console.error);
