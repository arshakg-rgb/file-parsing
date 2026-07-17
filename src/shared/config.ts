import Config from "../config/system-config/Config.js";

const config = Config.getInstance();

// Re-export settings from Config singleton for backward compatibility
export const settings = config.settings;

// Re-export Settings type for backward compatibility
export type Settings = typeof settings;
