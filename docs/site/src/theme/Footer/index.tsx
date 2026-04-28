import React from 'react';
import Link from '@docusaurus/Link';
import useBaseUrl from '@docusaurus/useBaseUrl';

const IconX = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
    <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-4.714-6.231-5.401 6.231H2.744l7.73-8.835L1.254 2.25H8.08l4.258 5.632L18.244 2.25zm-1.161 17.52h1.833L7.084 4.126H5.117L17.083 19.77z"/>
  </svg>
);

const IconDiscord = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
    <path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057c.002.022.014.043.03.058a19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028c.462-.63.874-1.295 1.226-1.994a.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z"/>
  </svg>
);

const IconGitHub = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
    <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.22-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12"/>
  </svg>
);

const IconLinkedIn = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="currentColor">
    <path d="M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433a2.062 2.062 0 0 1-2.063-2.065 2.064 2.064 0 1 1 2.063 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"/>
  </svg>
);

export default function Footer(): React.ReactElement {
  const logoUrl = useBaseUrl('/img/logo-icon-orange.png');

  return (
    <footer style={{background: '#000000', color: '#ffffff', fontFamily: "'Nunito Sans', sans-serif"}}>
      {/* Top section */}
      <div style={{
        maxWidth: '1280px',
        margin: '0 auto',
        padding: '3.5rem 2rem 2.5rem',
        display: 'grid',
        gridTemplateColumns: '2fr 1fr 1fr 1fr 1fr',
        gap: '2rem',
      }}>

        {/* Brand column */}
        <div style={{display: 'flex', flexDirection: 'column', gap: '1rem'}}>
          <div style={{display: 'flex', alignItems: 'center', gap: '0.6rem'}}>
            <img src={logoUrl} alt="Erigon" style={{height: '32px', width: 'auto'}} />
            <span style={{fontFamily: "'Quantify', sans-serif", fontWeight: 700, fontSize: '1rem', letterSpacing: '0.04em'}}>erigon.tech</span>
          </div>
          <p style={{
            fontSize: '0.875rem',
            color: 'rgba(255,255,255,0.55)',
            lineHeight: 1.6,
            margin: 0,
            maxWidth: '240px',
          }}>
            Building the future on the efficient software frontier.
          </p>
          <p style={{
            fontSize: '0.8rem',
            color: 'rgba(255,255,255,0.35)',
            lineHeight: 1.6,
            margin: 0,
          }}>
            Erigon Technologies AG<br />
            Dammstrasse 16<br />
            6300 Zug, Switzerland
          </p>
        </div>

        {/* Products column */}
        <div>
          <p style={{fontFamily: "'Montserrat', sans-serif", fontWeight: 800, fontSize: '0.8rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#f0f0f0', margin: '0 0 1rem'}}>Products</p>
          <ul style={{listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.6rem'}}>
            {[
              {label: 'Erigon Client', href: 'https://erigon.tech/products/erigon-client/'},
              {label: 'Zilkworm', href: 'https://erigon.tech/products/zilkworm/'},
              {label: 'Cocoon', href: 'https://erigon.tech/products/cocoon/'},
              {label: 'R&D', href: 'https://erigon.tech/products/rnd/'},
            ].map(({label, href}) => (
              <li key={label}>
                <a href={href} target="_blank" rel="noopener noreferrer" style={{color: 'rgba(255,255,255,0.65)', textDecoration: 'none', fontSize: '0.9rem', transition: 'color 0.15s'}}
                  onMouseEnter={e => (e.currentTarget.style.color = '#EF7716')}
                  onMouseLeave={e => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}>
                  {label}
                </a>
              </li>
            ))}
          </ul>
        </div>

        {/* Developers column */}
        <div>
          <p style={{fontFamily: "'Montserrat', sans-serif", fontWeight: 800, fontSize: '0.8rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#f0f0f0', margin: '0 0 1rem'}}>Developers</p>
          <ul style={{listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.6rem'}}>
            {[
              {label: 'Zilkworm Docs ↗', href: 'https://zilkworm.erigon.tech'},
              {label: 'Cocoon Docs ↗', href: 'https://cocoon.erigon.tech'},
              {label: 'Erigon Docs ↗', href: 'https://docs.erigon.tech'},
              {label: 'Blog', href: 'https://erigon.tech/blog/'},
            ].map(({label, href}) => (
              <li key={label}>
                <a href={href} target="_blank" rel="noopener noreferrer" style={{color: 'rgba(255,255,255,0.65)', textDecoration: 'none', fontSize: '0.9rem', transition: 'color 0.15s'}}
                  onMouseEnter={e => (e.currentTarget.style.color = '#EF7716')}
                  onMouseLeave={e => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}>
                  {label}
                </a>
              </li>
            ))}
          </ul>
        </div>

        {/* Company column */}
        <div>
          <p style={{fontFamily: "'Montserrat', sans-serif", fontWeight: 800, fontSize: '0.8rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#f0f0f0', margin: '0 0 1rem'}}>Company</p>
          <ul style={{listStyle: 'none', padding: 0, margin: 0, display: 'flex', flexDirection: 'column', gap: '0.6rem'}}>
            {[
              {label: 'About Us', href: 'https://erigon.tech/about/'},
              {label: 'Services', href: 'https://erigon.tech/services/'},
              {label: 'Contact', href: 'https://erigon.tech/contact/'},
              {label: 'Privacy Policy', href: 'https://erigon.tech/privacy/'},
              {label: 'Cookie Policy', href: 'https://erigon.tech/cookies/'},
            ].map(({label, href}) => (
              <li key={label}>
                <a href={href} target="_blank" rel="noopener noreferrer" style={{color: 'rgba(255,255,255,0.65)', textDecoration: 'none', fontSize: '0.9rem', transition: 'color 0.15s'}}
                  onMouseEnter={e => (e.currentTarget.style.color = '#EF7716')}
                  onMouseLeave={e => (e.currentTarget.style.color = 'rgba(255,255,255,0.65)')}>
                  {label}
                </a>
              </li>
            ))}
          </ul>
        </div>

        {/* Community column */}
        <div>
          <p style={{fontFamily: "'Montserrat', sans-serif", fontWeight: 800, fontSize: '0.8rem', letterSpacing: '0.08em', textTransform: 'uppercase', color: '#f0f0f0', margin: '0 0 1rem'}}>Community</p>
          <div style={{display: 'flex', flexDirection: 'column', gap: '0.5rem'}}>
            {[
              {label: 'X / Twitter', href: 'https://x.com/erigoneth', Icon: IconX},
              {label: 'Discord', href: 'https://dsc.gg/erigon', Icon: IconDiscord},
              {label: 'GitHub', href: 'https://github.com/erigontech', Icon: IconGitHub},
              {label: 'LinkedIn', href: 'https://www.linkedin.com/company/erigon/', Icon: IconLinkedIn},
            ].map(({label, href, Icon}) => (
              <a key={label} href={href} target="_blank" rel="noopener noreferrer" className="footer-social-btn">
                <Icon />
                {label}
              </a>
            ))}
          </div>
        </div>
      </div>

      {/* Divider */}
      <div style={{borderTop: '1px solid rgba(255,255,255,0.08)', margin: '0 2rem'}} />

      {/* Bottom bar */}
      <div style={{
        maxWidth: '1280px',
        margin: '0 auto',
        padding: '1.25rem 2rem',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: '0.75rem',
      }}>
        <span style={{fontSize: '0.8rem', color: 'rgba(255,255,255,0.35)'}}>
          © {new Date().getFullYear()} Erigon Technologies AG. All rights reserved.
        </span>
        <div style={{display: 'flex', alignItems: 'center', gap: '1.5rem', flexWrap: 'wrap'}}>
          {[
            {label: 'Privacy Policy', href: 'https://erigon.tech/privacy/'},
            {label: 'Cookie Policy', href: 'https://erigon.tech/cookies/'},
            {label: 'Contact', href: 'https://erigon.tech/contact/'},
          ].map(({label, href}) => (
            <a key={label} href={href} target="_blank" rel="noopener noreferrer"
              style={{fontSize: '0.8rem', color: 'rgba(255,255,255,0.35)', textDecoration: 'none', transition: 'color 0.15s'}}
              onMouseEnter={e => (e.currentTarget.style.color = '#EF7716')}
              onMouseLeave={e => (e.currentTarget.style.color = 'rgba(255,255,255,0.35)')}>
              {label}
            </a>
          ))}
          <a href="mailto:hello@erigon.tech"
            style={{fontSize: '0.8rem', color: 'rgba(255,255,255,0.35)', textDecoration: 'none', transition: 'color 0.15s'}}
            onMouseEnter={e => (e.currentTarget.style.color = '#EF7716')}
            onMouseLeave={e => (e.currentTarget.style.color = 'rgba(255,255,255,0.35)')}>
            hello@erigon.tech
          </a>
        </div>
      </div>
    </footer>
  );
}
