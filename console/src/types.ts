export interface Incident {
  id: number;
  issuerBin: string;
  windowStart: string;
  windowEnd: string;
  detectedAt: string;
  approvalRateObserved: number;
  windowVolume: number;
  dominantDeclineCode: string | null;
}

export interface TriggeringSegment {
  issuerBin: string;
  windowStart: string;
  windowEnd: string;
  totalAuthorizations: number;
  approvedCount: number;
  declinedCount: number;
  declineCodeCounts: Record<string, number>;
  approvalRate: number;
  decisionLatencyP50Nanos: number;
  decisionLatencyP99Nanos: number;
}

export interface IncidentDetail {
  incident: Incident;
  triggeringSegment: TriggeringSegment;
}
