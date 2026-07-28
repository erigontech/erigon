import React, {type ReactNode} from 'react';
import Layout from '@theme-original/DocItem/Layout';
import type LayoutType from '@theme/DocItem/Layout';
import type {WrapperProps} from '@docusaurus/types';
import Head from '@docusaurus/Head';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useDoc, useDocsVersion} from '@docusaurus/plugin-content-docs/client';

type Props = WrapperProps<typeof LayoutType>;

// Wraps every doc page (main docs + Help Center) to:
//  1. Emit TechArticle JSON-LD so search engines and AI assistants can extract
//     a clean title/description/URL per page (Docusaurus already emits
//     BreadcrumbList; this adds the article-level entity). Pages can opt out
//     with `structured_data: false` in front matter — e.g. the FAQ page, which
//     emits its own FAQPage schema instead.
//  2. Mark archived versions (banner === 'unmaintained') noindex so search
//     engines consolidate ranking signals on the current version rather than
//     splitting them across near-duplicate versioned pages. `follow` keeps the
//     links crawlable.
export default function LayoutWrapper(props: Props): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const {metadata, frontMatter} = useDoc();
  const {banner} = useDocsVersion();

  const isArchived = banner === 'unmaintained';
  // `structured_data: false` is a custom front-matter opt-out (e.g. the FAQ page,
  // which emits its own FAQPage schema). It isn't part of DocFrontMatter, so read
  // it from metadata.frontMatter, which carries an index signature for extra keys.
  const optedOut = (metadata.frontMatter.structured_data as boolean | undefined) === false;

  const url = new URL(metadata.permalink, siteConfig.url).toString();
  const description = (frontMatter.description as string | undefined) ?? metadata.description;

  const techArticle = {
    '@context': 'https://schema.org',
    '@type': 'TechArticle',
    headline: metadata.title,
    ...(description ? {description} : {}),
    url,
    inLanguage: 'en',
    isPartOf: {'@type': 'WebSite', name: siteConfig.title, url: siteConfig.url},
    publisher: {'@type': 'Organization', name: 'Erigon', url: 'https://erigon.tech'},
  };

  return (
    <>
      <Head>
        {isArchived && <meta name="robots" content="noindex, follow" />}
        {!optedOut && !isArchived && (
          <script type="application/ld+json">{JSON.stringify(techArticle)}</script>
        )}
      </Head>
      <Layout {...props} />
    </>
  );
}
