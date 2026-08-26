// node --test src/releases.test.js
//
// Every case here is a list ordered the way the GitHub API returns it — by
// publish date, newest first — so a selector that trusts that order fails.

const test = require('node:test');
const assert = require('node:assert/strict');

const {
  installableVersion,
  latestStableInSeries,
  stableInSeries,
  highestStable,
} = require('./releases.js');

const rel = (tag, extra = {}) => ({tag_name: tag, prerelease: false, draft: false, ...extra});

test('back-port published after a newer series does not win the fallback', () => {
  // v3.2.4 shipped after v3.3.0, so it heads the API list.
  const releases = [rel('v3.2.4'), rel('v3.3.0'), rel('v3.2.3')];
  assert.equal(installableVersion(releases, 'v3.6'), '3.3.0');
});

test('back-port published after a newer patch does not win its own series', () => {
  const releases = [rel('v3.4.3'), rel('v3.4.10'), rel('v3.4.9')];
  assert.equal(latestStableInSeries(releases, 'v3.4'), '3.4.10');
});

test('patch components compare numerically, not lexically', () => {
  const releases = [rel('v3.5.9'), rel('v3.5.10')];
  assert.equal(latestStableInSeries(releases, 'v3.5'), '3.5.10');
});

test('the current series wins over a higher one when it has a stable release', () => {
  const releases = [rel('v3.7.0'), rel('v3.6.1')];
  assert.equal(installableVersion(releases, 'v3.6'), '3.6.1');
});

test('a series prefix does not swallow a longer numeric series', () => {
  const releases = [rel('v3.60.0'), rel('v3.6.2')];
  assert.equal(latestStableInSeries(releases, 'v3.6'), '3.6.2');
  assert.equal(stableInSeries(releases, 'v3.6').tag_name, 'v3.6.2');
});

test('pre-release suffixes, prerelease flags and drafts are all rejected', () => {
  const releases = [
    rel('v3.7.0-rc.1'),
    rel('v3.7.0-dev'),
    rel('v3.6.9', {prerelease: true}),
    rel('v3.6.8', {draft: true}),
    rel('v3.6.2'),
  ];
  assert.equal(highestStable(releases).tag_name, 'v3.6.2');
  assert.equal(installableVersion(releases, 'v3.6'), '3.6.2');
});

test('a missing series throws rather than silently resolving elsewhere', () => {
  assert.throws(() => latestStableInSeries([rel('v3.6.2')], 'v3.3'), /No stable v3\.3 release/);
});

test('no stable release at all throws', () => {
  assert.throws(() => installableVersion([rel('v3.7.0-rc.1')], 'v3.6'), /No stable release/);
});
