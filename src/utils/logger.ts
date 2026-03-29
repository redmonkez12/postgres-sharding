type LogLevel = "info" | "warn" | "error";

function formatContext(context: Record<string, unknown> | undefined): string {
  if (!context || Object.keys(context).length === 0) {
    return "";
  }

  return ` ${JSON.stringify(context)}`;
}

export function log(level: LogLevel, message: string, context?: Record<string, unknown>): void {
  const line = `[${new Date().toISOString()}] ${level.toUpperCase()} ${message}${formatContext(context)}`;

  if (level === "error") {
    console.error(line);
    return;
  }

  console.log(line);
}

export const logger = {
  info: (message: string, context?: Record<string, unknown>) => log("info", message, context),
  warn: (message: string, context?: Record<string, unknown>) => log("warn", message, context),
  error: (message: string, context?: Record<string, unknown>) => log("error", message, context),
};
