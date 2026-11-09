import { useCallback, useEffect, useState } from 'react';
import type { Incident, IncidentDetail as IncidentDetailData } from './types';
import { fetchIncidentDetail, fetchIncidents } from './api';
import { IncidentList } from './components/IncidentList';
import { IncidentDetail } from './components/IncidentDetail';

export default function App() {
  const [incidents, setIncidents] = useState<Incident[]>([]);
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const [detail, setDetail] = useState<IncidentDetailData | null>(null);
  const [error, setError] = useState<string | null>(null);

  const loadList = useCallback(() => {
    fetchIncidents()
      .then(setIncidents)
      .catch((e) => setError(String(e.message ?? e)));
  }, []);

  useEffect(() => {
    loadList();
  }, [loadList]);

  useEffect(() => {
    if (selectedId == null) {
      setDetail(null);
      return;
    }
    fetchIncidentDetail(selectedId)
      .then(setDetail)
      .catch((e) => setError(String(e.message ?? e)));
  }, [selectedId]);

  return (
    <div className="app">
      <h1>Authorization Health Monitor</h1>
      {error ? <p data-testid="app-error">{error}</p> : null}
      {selectedId == null ? (
        <IncidentList incidents={incidents} onSelect={setSelectedId} />
      ) : (
        <IncidentDetail detail={detail} onBack={() => setSelectedId(null)} />
      )}
    </div>
  );
}
