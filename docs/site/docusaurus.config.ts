import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const versionReplace = require('./src/remark/version-replace.js');

// Archived doc versions (newest-first). Single source of truth: adding an entry
// here (via `docusaurus docs:version`) is all that's needed — version injection
// below derives everything from this list, no per-version config edits required.
const archivedVersions: string[] = require('./versions.json');

// The docs series this branch publishes; also scopes the {ERIGON_VERSION}
// lookup below. The `label: 'vX.Y'` literal is rewritten at each cutover.
const currentDocsVersion = {
  label: 'v3.6',
  badge: false,
};

type Release = {tag_name: string; prerelease: boolean; draft: boolean};

// Stable-release selection lives in its own CommonJS module so its ordering
// rules are testable without booting Docusaurus (src/releases.test.js). The
// GitHub API orders releases by publish date, so every selector there sorts by
// semantic version instead of trusting that order.
const {installableVersion, latestStableInSeries} = require('./src/releases.js');

function githubHeaders(): Record<string, string> {
  const headers: Record<string, string> = {Accept: 'application/vnd.github.v3+json'};
  if (process.env.GITHUB_TOKEN) headers['Authorization'] = `Bearer ${process.env.GITHUB_TOKEN}`;
  return headers;
}

// One page serves every series resolved below; 100 is the API maximum and must
// stay wide enough to reach the oldest archived series.
async function fetchReleases(): Promise<Release[]> {
  const res = await fetch(
    'https://api.github.com/repos/erigontech/erigon/releases?per_page=100',
    {headers: githubHeaders()},
  );
  if (!res.ok) {
    const hint = res.status === 403 || res.status === 429
      ? ' Set GITHUB_TOKEN if this is rate limiting.'
      : '';
    throw new Error(`Release lookup failed: ${res.status} ${res.statusText}.${hint}`);
  }
  return await res.json() as Release[];
}

export default async function createConfig(): Promise<Config> {
  const releases = await fetchReleases();
  const latestVersion = installableVersion(releases, currentDocsVersion.label);
  // Map each archived version id (e.g. "v3.4") to its latest patch release string.
  const versionStrings: Record<string, string> = Object.fromEntries(
    archivedVersions.map((v) => [v, latestStableInSeries(releases, v)]),
  );

  return {
    title: 'Erigon Documentation',
    tagline: 'Ethereum execution client',
    favicon: 'img/logo-icon-orange.png',
    url: 'https://docs.erigon.tech',
    baseUrl: '/',
    organizationName: 'erigontech',
    projectName: 'docs',
    trailingSlash: false,
    onBrokenLinks: 'throw',
    onBrokenMarkdownLinks: 'throw',
    onBrokenAnchors: 'throw',
    i18n: {defaultLocale: 'en', locales: ['en']},

    markdown: {mermaid: true},
    themes: ['@docusaurus/theme-mermaid'],

    customFields: {latestVersion},

    headTags: [
      {
        tagName: 'script',
        attributes: {
          async: 'true',
          src: 'https://plausible.io/js/pa-dn7VOPE-2G3BcX86ipmLC.js',
        },
      },
      {
        tagName: 'script',
        attributes: {},
        innerHTML: 'window.plausible=window.plausible||function(){(plausible.q=plausible.q||[]).push(arguments)},plausible.init=plausible.init||function(i){plausible.o=i||{}};plausible.init()',
      },
    ],

    plugins: [
      [
        '@docusaurus/plugin-client-redirects',
        {
          redirects: [
            // NAT moved out of the CLI Reference subfolder to a top-level page.
            {
              from: '/fundamentals/configuring-erigon/nat',
              to: '/fundamentals/nat',
            },
            // The Polygon easy-node guide is removed: 3.1.* is the last series
            // that officially supports Polygon. Inbound links land on the support
            // statement; the guide itself is still readable in the v3.4 archive.
            {
              from: '/get-started/easy-nodes/how-to-run-a-polygon-node',
              to: '/fundamentals/supported-networks',
            },
          ],
        },
      ],
      [
        '@docusaurus/plugin-content-docs',
        {
          id: 'help-center',
          path: 'help-center',
          routeBasePath: 'help-center',
          sidebarPath: './sidebars-help-center.ts',
          showLastUpdateTime: true,
        },
      ],
      [
        require.resolve('@easyops-cn/docusaurus-search-local'),
        {
          hashed: true,
          language: ['en'],
          docsRouteBasePath: ['/', '/help-center'],
          indexDocs: true,
          indexBlog: false,
          indexPages: false,
          searchBarPosition: 'right',
        },
      ],
    ],

    presets: [
      ['classic', {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: '/',
          lastVersion: 'current',
          versions: {
            current: currentDocsVersion,
          },
          remarkPlugins: [[versionReplace, {currentVersion: latestVersion, versionStrings}]],
          showLastUpdateTime: true,
        },
        blog: false as false,
        theme: {customCss: './src/css/custom.css'},
        sitemap: {
          // Emit <lastmod> per URL (from git history via showLastUpdateTime) so
          // crawlers can prioritise changed pages. Exclude /search from the
          // sitemap — it has no indexable content (the route itself still exists).
          lastmod: 'date',
          changefreq: 'weekly',
          priority: 0.5,
          ignorePatterns: ['/search'],
        },
      } satisfies Preset.Options],
    ],

    themeConfig: {
      navbar: {
        logo: {
          alt: 'Erigon',
          src: 'img/logo-icon-orange.png',
        },
        title: 'Erigon Client',
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docs',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'docSidebar',
            sidebarId: 'helpCenter',
            docsPluginId: 'help-center',
            position: 'left',
            label: 'Help Center',
          },
          {
            type: 'docsVersionDropdown',
            position: 'right',
            dropdownActiveClassDisabled: true,
          },
          {
            type: 'html',
            position: 'right',
            value: '<a href="https://erigon.tech/blog/" target="_blank" rel="noopener noreferrer" class="navbar-blog-btn" aria-label="Blog">Blog</a>',
          },
          {
            type: 'html',
            position: 'right',
            value: '<a href="https://github.com/erigontech/erigon/releases" target="_blank" rel="noopener noreferrer" class="navbar-release-btn" aria-label="Release Notes">Release Notes</a>',
          },
          {
            type: 'html',
            position: 'right',
            value: '<a href="https://github.com/erigontech/erigon" target="_blank" rel="noopener noreferrer" class="navbar-github-icon" aria-label="GitHub"><svg width="20" height="20" viewBox="0 0 24 24" fill="currentColor"><path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/></svg></a>',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {title: 'Community', items: [
            {label: 'GitHub', href: 'https://github.com/erigontech/erigon'},
            {label: 'Discord', href: 'https://discord.gg/erigon'},
          ]},
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Erigon. Built with Docusaurus.`,
      },
      metadata: [
        {name: 'description', content: 'Official documentation for Erigon — the efficient, modular Ethereum execution client built for performance and low disk footprint.'},
        {name: 'theme-color', content: '#EF7716'},
        {property: 'og:type', content: 'website'},
        {property: 'og:site_name', content: 'Erigon Documentation'},
        {property: 'og:image', content: 'https://docs.erigon.tech/img/og-image.png'},
        {name: 'twitter:card', content: 'summary_large_image'},
        {name: 'twitter:site', content: '@erigoneth'},
        {name: 'twitter:image', content: 'https://docs.erigon.tech/img/og-image.png'},
      ],

      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
      },
    } satisfies Preset.ThemeConfig,
  };
}
