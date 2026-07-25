// Remark plugin — replaces {ERIGON_VERSION} in text, inlineCode, code nodes,
// and also in mdxTextExpression nodes (plain-text {ERIGON_VERSION} in MDX files).
// Uses vfile.path to pick the right version: a `version-vX.Y` path segment selects
// versionStrings["vX.Y"]; everything else (current docs) gets currentVersion.
// Adding a new archived version needs no change here — it's keyed off versions.json.
function versionReplace(options) {
  const currentVersion = (options && options.currentVersion) || 'latest';
  const versionStrings = (options && options.versionStrings) || {};

  function visit(node, version) {
    if (
      node.type === 'text' ||
      node.type === 'inlineCode' ||
      node.type === 'code'
    ) {
      node.value = node.value.replace(/\{ERIGON_VERSION\}/g, version);
    }
    // {ERIGON_VERSION} in plain MDX text becomes an mdxTextExpression node.
    // Convert it to a plain text node with the resolved version.
    if (node.type === 'mdxTextExpression' && node.value === 'ERIGON_VERSION') {
      node.type = 'text';
      node.value = version;
    }
    if (node.children) {
      node.children.forEach((child) => visit(child, version));
    }
  }

  return function (tree, vfile) {
    const segments = (vfile && vfile.path && vfile.path.split('/')) || [];
    const versionSeg = segments.find((s) => s.startsWith('version-'));
    const key = versionSeg && versionSeg.slice('version-'.length); // e.g. "v3.4"
    const version = (key && versionStrings[key]) || currentVersion;
    visit(tree, version);
  };
}

module.exports = versionReplace;
