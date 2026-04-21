import { LegalShell, LegalSection } from '../components/LegalShell';

export const metadata = {
  title: 'Privacy Policy — 3M',
  description: 'How Mullin Capital collects, uses, and protects your information.',
};

export default function PrivacyPage() {
  return (
    <LegalShell title="Privacy Policy" effective="January 1, 2026">
      <p style={{ marginBottom: '2.5rem' }}>
        This Privacy Policy describes how Mullin Capital (&ldquo;we&rdquo;, &ldquo;us&rdquo;, or
        &ldquo;3M&rdquo;) collects, uses, and safeguards information when you access our quantitative
        research and trading platform. By using the service, you agree to the practices described below.
      </p>

      <LegalSection title="Information we collect">
        <p style={{ marginBottom: '1rem' }}>
          <strong style={{ color: 'var(--t0)' }}>Account information.</strong> Email address, name, firm
          affiliation, and any credentials you provide during signup or onboarding.
        </p>
        <p style={{ marginBottom: '1rem' }}>
          <strong style={{ color: 'var(--t0)' }}>Usage data.</strong> Pages viewed, features used,
          session metadata, device and browser information, and diagnostic logs needed to operate and
          improve the service.
        </p>
        <p>
          <strong style={{ color: 'var(--t0)' }}>Strategy and trading data.</strong> Any parameters,
          datasets, or API keys you upload or link. Exchange credentials are stored encrypted and are
          used solely to execute actions you authorize.
        </p>
      </LegalSection>

      <LegalSection title="How we use information">
        <p>
          We use collected information to operate, maintain, and improve the platform, to authenticate
          users, to provide support, to send service-related communications, and to comply with legal
          obligations. We do not sell personal information.
        </p>
      </LegalSection>

      <LegalSection title="Sharing and disclosure">
        <p>
          We share information only with service providers that host infrastructure, process payments,
          or deliver email on our behalf, and only under confidentiality obligations. We may disclose
          information when required by law, to protect our rights, or with your explicit consent.
        </p>
      </LegalSection>

      <LegalSection title="Data security">
        <p>
          We employ industry-standard safeguards including encryption in transit and at rest, access
          controls, and audit logging. No system is perfectly secure; you are responsible for keeping
          your credentials confidential.
        </p>
      </LegalSection>

      <LegalSection title="Your rights">
        <p>
          Depending on your jurisdiction, you may have the right to access, correct, export, or delete
          your personal information, and to object to certain processing. To exercise any of these
          rights, contact us at the address below.
        </p>
      </LegalSection>

      <LegalSection title="Retention">
        <p>
          We retain account and trading data for as long as your account is active and for a reasonable
          period thereafter to meet legal, accounting, or regulatory obligations.
        </p>
      </LegalSection>

      <LegalSection title="Changes to this policy">
        <p>
          We may update this policy from time to time. Material changes will be communicated by email
          or through the platform. Continued use of the service after the effective date of any update
          constitutes acceptance of the revised policy.
        </p>
      </LegalSection>

      <LegalSection title="Contact">
        <p>
          Questions about this policy or your data can be directed to{' '}
          <a href="mailto:j@mullincap.com" style={{ color: 'var(--green)', textDecoration: 'none' }}>
            j@mullincap.com
          </a>.
        </p>
      </LegalSection>
    </LegalShell>
  );
}
