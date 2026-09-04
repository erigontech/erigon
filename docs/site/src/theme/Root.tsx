import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useAllDocsData} from '@docusaurus/plugin-content-docs/client';

// The llms.txt index covers the current docs and help center only, so it must
// not be advertised on archived routes, nor on routes it does not list at all.
// A not-found page keeps the path that was requested rather than /404.html, so
// listing that route cannot exclude it. Nor can the active plugin: it is matched
// by base-path prefix, and one instance is mounted at '/', so every pathname has
// one. The descriptor is therefore emitted only for a pathname that exactly
// matches a document route the docs plugins declare — which is also the set
// llms.txt lists.
// `describedby` is the relation llmstxt.org defines for an index that describes
// a page; llms-full.txt describes no single page and is discoverable through
// llms.txt and the sitemap instead.
export default function Root({children}: {children: ReactNode}): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const archived = (siteConfig.customFields?.archivedVersions ?? []) as string[];
  const nonDocument = (siteConfig.customFields?.nonDocumentRoutes ?? []) as string[];
  const {pathname} = useLocation();
  const allDocsData = useAllDocsData();
  // Routing is case-insensitive and index docs keep a trailing slash in the
  // plugin data even under trailingSlash:false, so every comparison below is
  // made on the same normalised form. Comparing one of them raw advertises the
  // current index on an archived route spelled /V3.4/.
  const key = (r: string) =>
    (r.length > 1 ? r.replace(/\/$/, '') : r).toLowerCase();
  const here = key(pathname);
  const isArchived = archived.some(
    (v) => here === key(`/${v}`) || here.startsWith(`${key(`/${v}`)}/`),
  );
  const isNonDocument = nonDocument.some(
    (r) => here === key(r) || here.startsWith(`${key(r)}/`),
  );
  const isDocumentRoute = Object.values(allDocsData).some((plugin) =>
    plugin.versions.some((version) =>
      version.docs.some((doc) => key(doc.path) === here),
    ),
  );
  return (
    <>
      {isDocumentRoute && !isArchived && !isNonDocument && (
        <Head>
          <link
            rel="describedby"
            href="https://docs.erigon.tech/llms.txt"
            title="Erigon Documentation — page index for LLMs"
          />
        </Head>
      )}
      {children}
    </>
  );
}
