import React from 'react';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function Version(): React.ReactElement {
  const {siteConfig} = useDocusaurusContext();
  return <>{(siteConfig.customFields?.latestVersion as string) ?? 'latest'}</>;
}
