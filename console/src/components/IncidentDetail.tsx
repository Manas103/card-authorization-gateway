import type { IncidentDetail as IncidentDetailData } from '../types';

interface Props {
  detail: IncidentDetailData | null;
  onBack: () => void;
}

/**
 * Opens an incident directly on its triggering segment (the exact BIN and
 * rolling window that crossed the approval-rate floor) and its decline-code
 * mix, rather than requiring a second lookup.
 */
export function IncidentDetail({ detail, onBack }: Props) {
  if (!detail) {
    return <p data-testid="detail-loading">Loading...</p>;
  }
  const { incident, triggeringSegment } = detail;

  return (
    <section data-testid="incident-detail">
      <button onClick={onBack}>Back</button>
      <h2>Incident on {incident.issuerBin}</h2>
      <p data-testid="triggering-segment">
        Triggering segment: {triggeringSegment.windowStart} to {triggeringSegment.windowEnd}
      </p>
      <p data-testid="dominant-decline-code">Dominant decline code: {incident.dominantDeclineCode}</p>
      <p data-testid="approval-rate">
        Approval rate: {(triggeringSegment.approvalRate * 100).toFixed(1)}% over {triggeringSegment.totalAuthorizations} authorizations
      </p>
      <h3>Decline code mix</h3>
      <ul data-testid="decline-code-mix">
        {Object.entries(triggeringSegment.declineCodeCounts).map(([code, count]) => (
          <li key={code} data-testid={`decline-code-${code}`}>
            {code}: {count}
          </li>
        ))}
      </ul>
      <p data-testid="latency">
        p50 decision latency: {(triggeringSegment.decisionLatencyP50Nanos / 1000).toFixed(1)} us, p99:{' '}
        {(triggeringSegment.decisionLatencyP99Nanos / 1000).toFixed(1)} us
      </p>
    </section>
  );
}
