import { LegalShell, LegalSection } from '../components/LegalShell';

export const metadata = {
  title: 'Terms of Service — 3M',
  description: 'Terms governing access to and use of the 3M platform.',
};

export default function TermsPage() {
  return (
    <LegalShell title="Terms of Service" effective="January 1, 2026">
      <p style={{ marginBottom: '2.5rem' }}>
        These Terms of Service (&ldquo;Terms&rdquo;) govern your access to and use of the 3M platform
        operated by Mullin Capital (&ldquo;we&rdquo;, &ldquo;us&rdquo;). By creating an account or using
        the service, you agree to be bound by these Terms.
      </p>

      <LegalSection title="Eligibility and accounts">
        <p>
          You must be at least 18 years old and legally able to enter into a binding contract to use
          the service. You are responsible for maintaining the confidentiality of your credentials and
          for all activity under your account.
        </p>
      </LegalSection>

      <LegalSection title="Acceptable use">
        <p>
          You agree not to misuse the service, including by attempting to gain unauthorized access,
          reverse-engineering non-public components, disrupting operation, uploading malicious code, or
          using the service for any unlawful purpose. We may suspend or terminate accounts that violate
          these rules.
        </p>
      </LegalSection>

      <LegalSection title="Strategies and user content">
        <p>
          You retain all rights to strategies, datasets, and other content you upload. You grant us a
          limited license to host, process, and display that content solely to operate the service on
          your behalf. You are solely responsible for ensuring you have the right to upload anything
          you submit.
        </p>
      </LegalSection>

      <LegalSection title="Not investment advice">
        <p>
          The service provides analytics, simulations, and execution tooling for informational
          purposes. Nothing on the platform constitutes investment advice, a recommendation, or an
          offer to buy or sell any asset. Past performance is not indicative of future results.
          Trading digital assets involves substantial risk, including total loss of capital. You are
          solely responsible for all trading decisions and their outcomes.
        </p>
      </LegalSection>

      <LegalSection title="Third-party integrations">
        <p>
          The service may connect to third-party exchanges, data providers, or infrastructure. We are
          not responsible for the availability, accuracy, or conduct of any third party, and your use
          of those integrations is subject to their respective terms.
        </p>
      </LegalSection>

      <LegalSection title="Fees and billing">
        <p>
          Paid plans are billed in advance on the cycle disclosed at signup. Fees are non-refundable
          except as required by law. We may change pricing with at least 30 days&rsquo; notice; changes
          take effect at the start of your next billing period.
        </p>
      </LegalSection>

      <LegalSection title="Intellectual property">
        <p>
          The 3M platform, including all software, branding, and documentation, is owned by Mullin
          Capital and protected by applicable intellectual property laws. These Terms do not grant you
          any right to our trademarks or proprietary technology beyond the limited license to use the
          service.
        </p>
      </LegalSection>

      <LegalSection title="Disclaimers">
        <p>
          The service is provided &ldquo;as is&rdquo; and &ldquo;as available&rdquo; without warranties
          of any kind, express or implied, including merchantability, fitness for a particular
          purpose, and non-infringement. We do not warrant that the service will be uninterrupted,
          error-free, or meet your specific requirements.
        </p>
      </LegalSection>

      <LegalSection title="Limitation of liability">
        <p>
          To the maximum extent permitted by law, Mullin Capital and its affiliates are not liable for
          any indirect, incidental, special, consequential, or punitive damages, or for any loss of
          profits, revenue, data, or trading losses arising from your use of the service. Our
          aggregate liability for any claim will not exceed the amounts paid by you to us in the 12
          months preceding the claim.
        </p>
      </LegalSection>

      <LegalSection title="Termination">
        <p>
          You may cancel your account at any time. We may suspend or terminate access for violation of
          these Terms, non-payment, or for any other reason at our discretion. Provisions that by
          their nature should survive termination will survive.
        </p>
      </LegalSection>

      <LegalSection title="Governing law">
        <p>
          These Terms are governed by the laws of the jurisdiction in which Mullin Capital is
          organized, without regard to conflict-of-law principles. Any dispute will be resolved in the
          courts of that jurisdiction, unless otherwise required by applicable consumer-protection
          law.
        </p>
      </LegalSection>

      <LegalSection title="Changes to these terms">
        <p>
          We may update these Terms from time to time. Material changes will be communicated by email
          or through the platform. Continued use after the effective date of any update constitutes
          acceptance of the revised Terms.
        </p>
      </LegalSection>

      <LegalSection title="Contact">
        <p>
          Questions about these Terms can be directed to{' '}
          <a href="mailto:j@mullincap.com" style={{ color: 'var(--green)', textDecoration: 'none' }}>
            j@mullincap.com
          </a>.
        </p>
      </LegalSection>
    </LegalShell>
  );
}
