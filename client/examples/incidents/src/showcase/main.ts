import "../client.js";
// In the entry, so the page's shell is styled from its first frame.
import "./console.css";

// Loaded after the client starts, so the working set downloads while React
// loads.
void import("./console.js");
