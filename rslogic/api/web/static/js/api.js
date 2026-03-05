export async function getJSON(url) {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

export async function postJSON(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

export function prettyJSON(value) {
  return JSON.stringify(value, null, 2);
}

export function clampPercent(value) {
  return Math.max(0, Math.min(100, Number(value) || 0));
}

export function formatClock() {
  return new Date().toLocaleTimeString("en-US", { hour12: false });
}
