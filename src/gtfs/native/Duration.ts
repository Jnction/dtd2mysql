
/**
 * A duration in seconds
 */
export type Duration = number;

export function formatDuration(duration: number): string {
  const hours = Math.floor(duration / 3600);
  const hoursFormatted = hours < 10 ? "0" + hours : hours;
  const mins = Math.floor((duration % 3600) / 60);
  const minsFormatted = mins < 10 ? "0" + mins : mins;
  const secs = duration % 60;
  const secsFormatted = secs < 10 ? "0" + secs : secs;

  return `${hoursFormatted}:${minsFormatted}:${secsFormatted}`;
}
