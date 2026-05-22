import React from 'react';
import Link from '@docusaurus/Link';

const cards = [
  {label: 'Get Started',   desc: 'Installation, hardware requirements, and first-run guides.',       to: '/get-started/'},
  {label: 'Easy Nodes',    desc: 'One-command setup for Ethereum, Gnosis Chain, and Polygon.',       to: '/get-started/easy-nodes/'},
  {label: 'Fundamentals',  desc: 'Sync modes, Docker, configuration, and day-to-day operations.',    to: '/fundamentals/'},
  {label: 'Staking',       desc: 'Solo staking with Caplin or any external consensus client.',       to: '/staking/'},
  {label: 'Help Center',   desc: 'Troubleshooting guides, FAQs, and support resources.',             to: '/help-center/'},
  {label: 'Release Notes', desc: 'Changelog and release history on GitHub.',                         href: 'https://github.com/erigontech/erigon/releases'},
];

export default function NotFoundContent(): React.ReactElement {
  return (
    <main>
      {/* Hero */}
      <section className="hero-404">
        <div className="hero-404-number">404</div>

        <h1 className="hero-404-title">
          This page is off the chain.
        </h1>

        <p className="hero-404-desc">
          The page you are looking for does not exist, has been moved,
          or is still being built at the efficient software frontier.
        </p>

        <div className="hero-404-actions">
          <Link to="/" className="button-404-primary">Back to Docs</Link>
          <a href="https://erigon.tech/contact/" target="_blank" rel="noopener noreferrer" className="button-404-secondary">Contact Us</a>
        </div>
      </section>

      {/* Cards */}
      <section className="cards-404-section">
        <h2 className="cards-404-heading">
          Where would you like to go?
        </h2>
        <div className="cards-404-grid">
          {cards.map(({label, desc, to, href}) => {
            const inner = (
              <>
                <div className="card-404-label">{label}</div>
                <div className="card-404-desc">{desc}</div>
              </>
            );
            return href ? (
              <a key={label} href={href} target="_blank" rel="noopener noreferrer" className="card-404">{inner}</a>
            ) : (
              <Link key={label} to={to!} className="card-404">{inner}</Link>
            );
          })}
        </div>
      </section>
    </main>
  );
}
