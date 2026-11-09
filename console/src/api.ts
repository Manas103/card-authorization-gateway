import type { Incident, IncidentDetail } from './types';

async function handle<T>(res: Response): Promise<T> {
  if (!res.ok) {
    throw new Error(`request failed with ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export function fetchIncidents(): Promise<Incident[]> {
  return fetch('/api/health/incidents').then((r) => handle<Incident[]>(r));
}

export function fetchIncidentDetail(id: number): Promise<IncidentDetail> {
  return fetch(`/api/health/incidents/${id}`).then((r) => handle<IncidentDetail>(r));
}
