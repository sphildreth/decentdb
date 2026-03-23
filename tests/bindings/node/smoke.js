const path = require("node:path");

const addon = require(path.join(__dirname, "smoke.node"));

if (!addon.runSmoke()) {
  throw new Error("node smoke returned false");
}
