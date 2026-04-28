// Remark plugin — replaces {ERIGON_VERSION} in all text, inlineCode, and code nodes.
// Configured in docusaurus.config.ts with the version fetched from GitHub at build time.
function versionReplace(options) {
  const version = (options && options.version) || 'latest';

  function visit(node) {
    if (
      node.type === 'text' ||
      node.type === 'inlineCode' ||
      node.type === 'code'
    ) {
      node.value = node.value.replace(/\{ERIGON_VERSION\}/g, version);
    }
    if (node.children) {
      node.children.forEach(visit);
    }
  }

  return function (tree) {
    visit(tree);
  };
}

module.exports = versionReplace;
