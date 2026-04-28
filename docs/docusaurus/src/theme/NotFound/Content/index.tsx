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
      <section style={{padding: '6rem 2rem 5rem', textAlign: 'center'}}>
        <div style={{
          fontFamily: "'Montserrat', sans-serif", fontWeight: 800,
          fontSize: 'clamp(5rem, 18vw, 10rem)', lineHeight: 1,
          color: '#EF7716', marginBottom: '1.5rem',
        }}>404</div>

        <h1 style={{
          fontFamily: "'Montserrat', sans-serif", fontWeight: 800,
          fontSize: 'clamp(1.5rem, 4vw, 2.25rem)', margin: '0 0 1rem',
        }}>
          This page is off the chain.
        </h1>

        <p style={{
          fontSize: '1.05rem', color: 'var(--ifm-color-emphasis-700)',
          maxWidth: '480px', margin: '0 auto 2.5rem', lineHeight: 1.65,
        }}>
          The page you are looking for does not exist, has been moved,
          or is still being built at the efficient software frontier.
        </p>

        <div style={{display: 'flex', gap: '1rem', justifyContent: 'center', flexWrap: 'wrap'}}>
          <Link to="/" className="button-404-primary">Back to Docs</Link>
          <a href="https://erigon.tech/contact/" target="_blank" rel="noopener noreferrer" className="button-404-secondary">Contact Us</a>
        </div>
      </section>

      {/* Cards */}
      <section style={{maxWidth: '1100px', margin: '0 auto', padding: '3rem 2rem 5rem'}}>
        <h2 style={{
          fontFamily: "'Montserrat', sans-serif", fontWeight: 800,
          fontSize: '1.1rem', marginBottom: '1.5rem',
          color: 'var(--ifm-color-emphasis-600)',
          textTransform: 'uppercase', letterSpacing: '0.07em',
        }}>
          Where would you like to go?
        </h2>
        <div style={{display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(260px, 1fr))', gap: '1rem'}}>
          {cards.map(({label, desc, to, href}) => {
            const inner = (
              <>
                <div style={{fontWeight: 700, fontSize: '0.95rem', marginBottom: '0.35rem', color: 'var(--ifm-font-color-base)'}}>{label}</div>
                <div style={{fontSize: '0.85rem', color: 'var(--ifm-color-emphasis-700)', lineHeight: 1.5}}>{desc}</div>
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
