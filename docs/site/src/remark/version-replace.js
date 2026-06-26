// Remark plugin — replaces {ERIGON_VERSION} in text, inlineCode, code nodes,
// and also in mdxTextExpression nodes (plain-text {ERIGON_VERSION} in MDX files).
// Uses vfile.path to pick the right version: v3.3 versioned docs get v33Version,
// v3.4 versioned docs get v34Version, everything else (current docs) gets currentVersion.
function versionReplace(options) {
  const currentVersion = (options && options.currentVersion) || 'latest';
  const v33Version = (options && options.v33Version) || 'latest';
  const v34Version = (options && options.v34Version) || 'latest';

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
    let version = currentVersion;
    if (segments.includes('version-v3.3')) version = v33Version;
    else if (segments.includes('version-v3.4')) version = v34Version;
    visit(tree, version);
  };
}

module.exports = versionReplace;
