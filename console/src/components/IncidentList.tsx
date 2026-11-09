import type { Incident } from '../types';

interface Props {
  incidents: Incident[];
  onSelect: (id: number) => void;
}

export function IncidentList({ incidents, onSelect }: Props) {
  return (
    <section>
      <h2>Incidents</h2>
      <ul data-testid="incident-list">
        {incidents.map((incident) => (
          <li key={incident.id} data-testid={`incident-row-${incident.id}`}>
            <span data-testid={`incident-row-bin-${incident.id}`}>{incident.issuerBin}</span>{' '}
            <span data-testid={`incident-row-code-${incident.id}`}>{incident.dominantDeclineCode}</span>{' '}
            <span>{(incident.approvalRateObserved * 100).toFixed(1)}% approved</span>{' '}
            <button data-testid={`open-incident-${incident.id}`} onClick={() => onSelect(incident.id)}>
              Open
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}
