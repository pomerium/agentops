package agentlink

func StreamID(s *Server, runID string) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if run := s.runs[runID]; run != nil {
		return run.streamID
	}
	return nil
}
