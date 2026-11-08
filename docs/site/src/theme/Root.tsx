import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import {useAllDocsData} from '@docusaurus/plugin-content-docs/client';

// Advertises the llms.txt index that covers the current documentation. The
// descriptor is emitted only on a route that index lists: archived versions
// describe themselves, and a route no docs plugin owns is not documentation at
// all. llms-full.txt gets no relation of its own — it describes the site rather
// than the page — and stays reachable through llms.txt and the sitemap.
export default function Root({children}: {children: ReactNode}): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const archived = (siteConfig.customFields?.archivedVersions ?? []) as string[];
  const {pathname} = useLocation();
  const allDocsData = useAllDocsData();
  // Every comparison below uses one normalised form: routing is
  // case-insensitive and index docs keep a trailing slash in the plugin data.
  const key = (r: string) =>
    (r.length > 1 ? r.replace(/\/$/, '') : r).toLowerCase();
  const here = key(pathname);
  const isArchived = archived.some(
    (v) => here === key(`/${v}`) || here.startsWith(`${key(`/${v}`)}/`),
  );
  const isDocumentRoute = Object.values(allDocsData).some((plugin) =>
    plugin.versions.some((version) =>
      version.docs.some((doc) => key(doc.path) === here),
    ),
  );
  return (
    <>
      {isDocumentRoute && !isArchived && (
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
