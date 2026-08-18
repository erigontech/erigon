import React, {type ReactNode} from 'react';
import Head from '@docusaurus/Head';
import {useLocation} from '@docusaurus/router';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

// Point every page at the llms.txt index that covers it. `describedby` is the
// relation the llmstxt.org proposal defines for exactly this; its `alternate` +
// `text/markdown` pairing is reserved for a *per-page* Markdown representation,
// which this site-wide index is not. llms-full.txt is deliberately not
// advertised: it describes no single page. It stays discoverable through
// llms.txt, the sitemap, and the MCP docs page.
//
// Emitted here rather than from config-level `headTags` because those go out on
// every route, archived versions included. generate-llms.py never walks
// versioned_docs, so llms.txt covers only the current docs and the help center;
// advertising it from /v3.4/** would send an agent reading an archived page to
// current, version-specific guidance. Archived routes therefore opt out, until
// there are per-version indexes to point them at instead.
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
