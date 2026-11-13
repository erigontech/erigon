import React from 'react';
import {translate} from '@docusaurus/Translate';
import {PageMetadata} from '@docusaurus/theme-common';
import Layout from '@theme/Layout';
import NotFoundContent from '@theme/NotFound/Content';

export default function NotFound(): React.ReactElement {
  return (
    <>
      <PageMetadata title={translate({id: 'theme.NotFound.title', message: 'Page Not Found'})} />
      <Layout noFooter>
        <NotFoundContent />
      </Layout>
    </>
  );
}
