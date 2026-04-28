// Remark plugin — replaces {ERIGON_VERSION} in all text, inlineCode, and code nodes.
// Uses vfile.path to pick the right version: v3.3 versioned docs get v33Version,
// everything else (current docs) gets currentVersion.
function versionReplace(options) {
  const currentVersion = (options && options.currentVersion) || 'latest';
  const v33Version = (options && options.v33Version) || 'latest';

  function visit(node, version) {
    if (
      node.type === 'text' ||
      node.type === 'inlineCode' ||
      node.type === 'code'
    ) {
      node.value = node.value.replace(/\{ERIGON_VERSION\}/g, version);
    }
    if (node.children) {
      node.children.forEach((child) => visit(child, version));
    }
  }

  return function (tree, vfile) {
    const isV33 = vfile && vfile.path && vfile.path.includes('version-v3.3');
    visit(tree, isV33 ? v33Version : currentVersion);
  };
}

module.exports = versionReplace;
