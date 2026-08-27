// Release selection for docusaurus.config.ts.
//
// Lives in its own module so the ordering rules below can be exercised without
// booting Docusaurus: `npm test` from docs/site.

// Releases carry a pre-release suffix without reliably setting the API's
// `prerelease` flag, so tag shape is the load-bearing check. Semver identifiers
// are arbitrary (-rc.1, -dev, -nightly), so reject any hyphen rather than a list.
const PRERELEASE_TAG = /-/;

const isStable = (r) =>
  !r.draft && !r.prerelease && !PRERELEASE_TAG.test(r.tag_name);

// Numeric components of a tag; a missing component reads as 0 so "v3.6" orders
// below "v3.6.1". Non-numeric junk also reads as 0 rather than NaN, which would
// make the comparison non-transitive.
function versionParts(tag) {
  return tag.replace(/^v/, '').split('.').map((n) => {
    const parsed = Number.parseInt(n, 10);
    return Number.isNaN(parsed) ? 0 : parsed;
  });
}

// Descending semantic order. The API lists releases by publish date, so any
// date-ordered pick is wrong the moment a back-port ships after a newer series
// (v3.2.4 published after v3.3.0). Every selector below sorts rather than
// taking the first match.
function compareDesc(a, b) {
  const left = versionParts(a.tag_name);
  const right = versionParts(b.tag_name);
  for (let i = 0; i < Math.max(left.length, right.length); i++) {
    const diff = (right[i] ?? 0) - (left[i] ?? 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

// Highest stable release, optionally restricted to a subset.
function highestStable(releases, predicate = () => true) {
  return releases.filter((r) => isStable(r) && predicate(r)).sort(compareDesc)[0];
}

// The trailing dot keeps series "v3.6" from swallowing "v3.60.0".
function stableInSeries(releases, series) {
  return highestStable(releases, (r) => r.tag_name.startsWith(`${series}.`));
}

function latestStableInSeries(releases, series) {
  const match = stableInSeries(releases, series);
  if (!match) {
    throw new Error(
      `No stable ${series} release found in the newest 100 releases. ` +
      'If that series predates the newest 100 releases, fetchReleases() needs pagination.',
    );
  }
  return match.tag_name.replace(/^v/, '');
}

// Version pasted into the install commands. Falls back to the highest stable
// release of any series while this one has none — with the sort above that is
// the newest stable series, not whatever was published most recently. Throwing
// is deliberate when nothing resolves at all.
function installableVersion(releases, series) {
  const match = stableInSeries(releases, series) ?? highestStable(releases);
  if (!match) {
    throw new Error('No stable release found in the newest 100 releases.');
  }
  return match.tag_name.replace(/^v/, '');
}

module.exports = {
  isStable,
  compareDesc,
  highestStable,
  stableInSeries,
  latestStableInSeries,
  installableVersion,
};
