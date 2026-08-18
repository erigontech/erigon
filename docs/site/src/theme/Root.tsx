import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

// The llms.txt index covers the current docs and help center only, so it must
// not be advertised on archived routes. `describedby` is the relation
// llmstxt.org defines for an index that describes a page; llms-full.txt
// describes no single page and is discoverable through llms.txt and the
// sitemap instead.
export default function Root({children}: {children: ReactNode}): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  const archived = (siteConfig.customFields?.archivedVersions ?? []) as string[];
  const {pathname} = useLocation();
  const isArchived = archived.some(
    (v) => pathname === `/${v}` || pathname.startsWith(`/${v}/`),
  );
  return (
    <>
      {!isArchived && (
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
