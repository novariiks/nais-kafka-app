import { Component, type ErrorInfo, type ReactNode } from 'react';

// Faro starts after the first render (see main.tsx), so the boundary reports through
// a reporter it sets once ready. Before that, errors only reach the console.
let report: (error: Error) => void = () => {};

export function setErrorReporter(reporter: (error: Error) => void): void {
  report = reporter;
}

/** Catches rendering errors, which React otherwise loses, and reports them to Faro. */
export class ErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error(error, info.componentStack);
    report(error);
  }

  render() {
    if (!this.state.failed) return this.props.children;
    return <div className="empty" role="alert"><h2>Something went wrong.</h2><p>Reload the page to try again.</p></div>;
  }
}
