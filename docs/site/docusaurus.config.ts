import {themes as prismThemes} from 'prism-react-renderer';
import type {Config} from '@docusaurus/types';
import type * as Preset from '@docusaurus/preset-classic';

const versionReplace = require('./src/remark/version-replace.js');

function githubHeaders(): Record<string, string> {
  const headers: Record<string, string> = {Accept: 'application/vnd.github.v3+json'};
  if (process.env.GITHUB_TOKEN) headers['Authorization'] = `Bearer ${process.env.GITHUB_TOKEN}`;
  return headers;
}

async function fetchLatestVersion(): Promise<string> {
  try {
    const res = await fetch(
      'https://api.github.com/repos/erigontech/erigon/releases/latest',
      {headers: githubHeaders()},
    );
    if (!res.ok) return 'latest';
    const data = await res.json() as {tag_name?: string};
    return data.tag_name?.replace(/^v/, '') ?? 'latest';
  } catch {
    return 'latest';
  }
}

async function fetchLatestV33Version(): Promise<string> {
  try {
    const res = await fetch(
      'https://api.github.com/repos/erigontech/erigon/releases?per_page=50',
      {headers: githubHeaders()},
    );
    if (!res.ok) return 'latest';
    const releases = await res.json() as Array<{tag_name: string; prerelease: boolean}>;
    const latest = releases.find((r) => !r.prerelease && r.tag_name.startsWith('v3.3.'));
    return latest?.tag_name.replace(/^v/, '') ?? 'latest';
  } catch {
    return 'latest';
  }
}

export default async function createConfig(): Promise<Config> {
  const [latestVersion, v33Version] = await Promise.all([
    fetchLatestVersion(),
    fetchLatestV33Version(),
  ]);

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
        '@docusaurus/plugin-content-docs',
        {
          id: 'help-center',
          path: 'help-center',
          routeBasePath: 'help-center',
          sidebarPath: './sidebars-help-center.ts',
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
      'docusaurus-plugin-llms-txt',
    ],

    presets: [
      ['classic', {
        docs: {
          sidebarPath: './sidebars.ts',
          routeBasePath: '/',
          lastVersion: 'current',
          versions: {
            current: {
              label: 'v3.4',
              badge: false,
            },
          },
          remarkPlugins: [[versionReplace, {currentVersion: latestVersion, v33Version}]],
        },
        blog: false as false,
        theme: {customCss: './src/css/custom.css'},
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
