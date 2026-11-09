import { render, screen } from '@testing-library/react';
import '@testing-library/jest-dom';
import { describe, expect, it, vi } from 'vitest';
import { IncidentDetail } from './IncidentDetail';
import type { IncidentDetail as IncidentDetailData } from '../types';

const DETAIL: IncidentDetailData = {
  incident: {
    id: 7,
    issuerBin: 'BIN-000042',
    windowStart: '2026-09-01T00:10:00Z',
    windowEnd: '2026-09-01T00:10:10Z',
    detectedAt: '2026-09-01T00:10:10Z',
    approvalRateObserved: 0.2,
    windowVolume: 100,
    dominantDeclineCode: 'SUSPECTED_FRAUD',
  },
  triggeringSegment: {
    issuerBin: 'BIN-000042',
    windowStart: '2026-09-01T00:10:00Z',
    windowEnd: '2026-09-01T00:10:10Z',
    totalAuthorizations: 100,
    approvedCount: 20,
    declinedCount: 80,
    declineCodeCounts: { SUSPECTED_FRAUD: 70, INVALID_PIN: 10 },
    approvalRate: 0.2,
    decisionLatencyP50Nanos: 5000,
    decisionLatencyP99Nanos: 9000,
  },
};

describe('IncidentDetail', () => {
  it('shows a loading state until a detail is fetched', () => {
    render(<IncidentDetail detail={null} onBack={vi.fn()} />);
    expect(screen.getByTestId('detail-loading')).toBeInTheDocument();
  });

  it('opens directly on the triggering segment and dominant decline code', () => {
    render(<IncidentDetail detail={DETAIL} onBack={vi.fn()} />);

    expect(screen.getByTestId('triggering-segment')).toHaveTextContent('2026-09-01T00:10:00Z');
    expect(screen.getByTestId('triggering-segment')).toHaveTextContent('2026-09-01T00:10:10Z');
    expect(screen.getByTestId('dominant-decline-code')).toHaveTextContent('SUSPECTED_FRAUD');
  });

  it('renders the full decline-code mix for the triggering segment, not just the dominant one', () => {
    render(<IncidentDetail detail={DETAIL} onBack={vi.fn()} />);

    expect(screen.getByTestId('decline-code-SUSPECTED_FRAUD')).toHaveTextContent('70');
    expect(screen.getByTestId('decline-code-INVALID_PIN')).toHaveTextContent('10');
  });
});
