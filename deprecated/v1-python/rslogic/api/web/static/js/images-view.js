import { deleteJSON, getJSON, postJSON, prettyJSON } from "./api.js";
import { element, listRowButton, textStack } from "./ui-components.js";

const OSM_TILE_URL = "https://tile.openstreetmap.org/{z}/{x}/{y}.png";
const DEFAULT_CENTER = [20, 0];
const DEFAULT_ZOOM = 2;
const FIT_PADDING = [48, 48];
const MARKER_SIZE = 12;
const MARKER_SAMPLE_LIMIT = 25;

function uniqueSorted(values) {
  return [...new Set(values)].sort();
}

function clampLatitude(value) {
  return Math.max(-85, Math.min(85, Number(value)));
}

function normalizeLongitude(value) {
  let longitude = Number(value);
  while (longitude < -180) longitude += 360;
  while (longitude > 180) longitude -= 360;
  return longitude;
}

function fitConfigForAssets(assets) {
  if (!assets.length) {
    return { center: DEFAULT_CENTER, bounds: null };
  }
  const latitudes = assets.map((asset) => clampLatitude(asset.latitude));
  const longitudes = assets.map((asset) => normalizeLongitude(asset.longitude));
  const minLat = Math.min(...latitudes);
  const maxLat = Math.max(...latitudes);
  const minLon = Math.min(...longitudes);
  const maxLon = Math.max(...longitudes);
  return {
    center: [(minLat + maxLat) / 2, (minLon + maxLon) / 2],
    bounds: [
      [minLat, minLon],
      [maxLat, maxLon],
    ],
  };
}

export function createImagesView(elements, hooks) {
  const state = {
    assets: [],
    groups: [],
    activeGroupId: "",
    activeGroupImageIds: new Set(),
    selectedImageIds: new Set(),
    drag: null,
    map: null,
    tileLayer: null,
    markers: [],
    fitConfig: { center: DEFAULT_CENTER, bounds: null },
    initialFitDone: false,
    renderScheduled: false,
    diagnostics: {
      leaflet_loaded: false,
      map_constructed: false,
      map_ready: false,
      tile_layer_added: false,
      tile_loads: 0,
      tile_errors: 0,
      container_size: null,
      last_error: null,
      last_tile_event: null,
      last_render: null,
    },
  };

  function leaflet() {
    const library = window.L;
    if (!library) {
      throw new Error("leaflet is not loaded");
    }
    state.diagnostics.leaflet_loaded = true;
    return library;
  }

  function selectionMode() {
    return elements.imagesSelectionMode.value || "replace";
  }

  function activeGroup() {
    return state.groups.find((group) => group.id === state.activeGroupId) || null;
  }

  function selectedAssets() {
    return state.assets.filter((asset) => state.selectedImageIds.has(asset.id));
  }

  function setImagesBadge(status, label) {
    elements.imagesStateBadge.className = `state-badge state-${status}`;
    elements.imagesStateBadge.textContent = label;
  }

  function reportMapError(error) {
    const message = error instanceof Error ? error.message : String(error);
    state.diagnostics.last_error = message;
    updateSummary();
    hooks.setStatus(`images map: ${message}`, "error");
  }

  function mapIsVisible() {
    const rect = elements.imagesGlobe.getBoundingClientRect();
    state.diagnostics.container_size = [Math.round(rect.width), Math.round(rect.height)];
    return rect.width > 0 && rect.height > 0;
  }

  function markerClass(asset) {
    const classes = ["images-point"];
    if (asset.group_ids?.length) {
      classes.push("is-in-group");
    }
    if (state.activeGroupImageIds.has(asset.id)) {
      classes.push("is-active-group");
    }
    if (state.selectedImageIds.has(asset.id)) {
      classes.push("is-selected");
    }
    return classes.join(" ");
  }

  function markerIcon(className) {
    const L = leaflet();
    return L.divIcon({
      className: `images-marker-shell ${className}`,
      html: '<span class="images-point-ring"></span>',
      iconSize: [MARKER_SIZE, MARKER_SIZE],
      iconAnchor: [MARKER_SIZE / 2, MARKER_SIZE / 2],
    });
  }

  function updateSummary() {
    const center = state.map ? state.map.getCenter() : { lat: state.fitConfig.center[0], lng: state.fitConfig.center[1] };
    const zoom = state.map ? Number(state.map.getZoom().toFixed(2)) : null;
    state.diagnostics.last_render = {
      asset_count: state.assets.length,
      marker_count: state.markers.length,
      selected_count: state.selectedImageIds.size,
    };
    elements.imagesMapSummary.textContent =
      `${state.selectedImageIds.size} selected | ${state.assets.length} mapped assets | ${state.groups.length} groups`;
    elements.imagesSelectionLog.textContent = prettyJSON({
      active_group_id: state.activeGroupId || null,
      active_group_name: activeGroup()?.name || null,
      selection_mode: selectionMode(),
      selected_image_ids: uniqueSorted([...state.selectedImageIds]),
      selected_filenames: selectedAssets().map((asset) => asset.filename || asset.id).slice(0, MARKER_SAMPLE_LIMIT),
      view_center: [Number(center.lng.toFixed(6)), Number(center.lat.toFixed(6))],
      view_zoom: zoom,
      map_diagnostics: state.diagnostics,
    });
  }

  function wireTileDiagnostics(tileLayer) {
    tileLayer.on("tileloadstart", (event) => {
      state.diagnostics.last_tile_event = { type: "tileloadstart", src: event.tile?.src || null };
      updateSummary();
    });
    tileLayer.on("tileload", (event) => {
      state.diagnostics.tile_loads += 1;
      state.diagnostics.last_tile_event = { type: "tileload", src: event.tile?.src || null };
      updateSummary();
    });
    tileLayer.on("tileerror", (event) => {
      state.diagnostics.tile_errors += 1;
      state.diagnostics.last_tile_event = { type: "tileerror", src: event.tile?.src || null };
      reportMapError(event.error || "leaflet tile error");
    });
  }

  function ensureMap() {
    if (state.map) {
      return state.map;
    }
    if (!mapIsVisible()) {
      throw new Error("images map is not visible");
    }
    const L = leaflet();
    state.map = L.map(elements.imagesGlobe, {
      zoomControl: false,
      zoomSnap: 0.25,
      worldCopyJump: true,
    });
    state.map.setView(state.fitConfig.center, DEFAULT_ZOOM);
    L.control.zoom({ position: "topright" }).addTo(state.map);
    state.tileLayer = L.tileLayer(OSM_TILE_URL, {
      attribution: "&copy; OpenStreetMap contributors",
      maxZoom: 19,
      crossOrigin: true,
    }).addTo(state.map);
    wireTileDiagnostics(state.tileLayer);
    state.map.whenReady(() => {
      state.diagnostics.map_ready = true;
      updateSummary();
    });
    state.map.on("moveend", updateSummary);
    state.map.on("resize", updateSummary);
    state.diagnostics.map_constructed = true;
    state.diagnostics.tile_layer_added = true;
    return state.map;
  }

  function renderGroups() {
    elements.imagesGroupsList.replaceChildren();
    for (const group of state.groups) {
      const button = listRowButton({
        className: "image-group-row",
        selected: group.id === state.activeGroupId,
        children: [
          textStack(group.name, `${group.image_count} images`, "image-group-main", "image-group-row-meta"),
          element("span", { text: group.id.slice(0, 8) }),
        ],
      });
      button.addEventListener("click", () => {
        state.activeGroupId = group.id;
        loadGroupDetail(group.id).catch((error) => hooks.setStatus(String(error), "error"));
      });
      elements.imagesGroupsList.append(button);
    }
  }

  function clearMarkers() {
    for (const entry of state.markers) {
      entry.marker.remove();
    }
    state.markers = [];
  }

  function renderMarkers() {
    const L = leaflet();
    const map = ensureMap();
    clearMarkers();
    for (const asset of state.assets) {
      const marker = L.marker(
        [clampLatitude(asset.latitude), normalizeLongitude(asset.longitude)],
        { icon: markerIcon(markerClass(asset)), keyboard: false },
      );
      marker.bindTooltip(`${asset.filename || asset.id}`, { direction: "top" });
      marker.addTo(map);
      state.markers.push({ asset, marker });
    }
  }

  function updateMarkerStyles() {
    for (const entry of state.markers) {
      entry.marker.setIcon(markerIcon(markerClass(entry.asset)));
    }
  }

  function applySelection(newIds) {
    const incoming = new Set(newIds);
    if (selectionMode() === "replace") {
      state.selectedImageIds = incoming;
    } else if (selectionMode() === "add") {
      for (const imageId of incoming) {
        state.selectedImageIds.add(imageId);
      }
    } else {
      for (const imageId of incoming) {
        state.selectedImageIds.delete(imageId);
      }
    }
    updateMarkerStyles();
    updateSummary();
  }

  function selectionBounds(event) {
    const rect = elements.imagesMapSurface.getBoundingClientRect();
    return {
      left: Math.min(state.drag.startX, event.clientX) - rect.left,
      top: Math.min(state.drag.startY, event.clientY) - rect.top,
      width: Math.abs(event.clientX - state.drag.startX),
      height: Math.abs(event.clientY - state.drag.startY),
    };
  }

  function showSelectionRect(bounds) {
    elements.imagesSelectionRect.style.display = "block";
    elements.imagesSelectionRect.style.left = `${bounds.left}px`;
    elements.imagesSelectionRect.style.top = `${bounds.top}px`;
    elements.imagesSelectionRect.style.width = `${bounds.width}px`;
    elements.imagesSelectionRect.style.height = `${bounds.height}px`;
  }

  function hideSelectionRect() {
    elements.imagesSelectionRect.style.display = "none";
    elements.imagesSelectionRect.style.width = "0";
    elements.imagesSelectionRect.style.height = "0";
  }

  function selectedIdsFromBounds(bounds) {
    if (!state.map || bounds.width < 4 || bounds.height < 4) {
      return [];
    }
    const left = bounds.left;
    const right = bounds.left + bounds.width;
    const top = bounds.top;
    const bottom = bounds.top + bounds.height;
    const selected = [];
    for (const entry of state.markers) {
      const point = state.map.latLngToContainerPoint(entry.marker.getLatLng());
      if (point.x >= left && point.x <= right && point.y >= top && point.y <= bottom) {
        selected.push(entry.asset.id);
      }
    }
    return selected;
  }

  function fitToAssets() {
    const map = ensureMap();
    map.invalidateSize(false);
    if (!state.assets.length) {
      map.setView(DEFAULT_CENTER, DEFAULT_ZOOM, { animate: false });
      updateSummary();
      return;
    }
    if (state.assets.length === 1) {
      const asset = state.assets[0];
      map.setView([clampLatitude(asset.latitude), normalizeLongitude(asset.longitude)], 17, { animate: false });
      updateSummary();
      return;
    }
    map.fitBounds(state.fitConfig.bounds, { padding: FIT_PADDING, animate: false, maxZoom: 17 });
    updateSummary();
  }

  async function loadGroups() {
    const payload = await getJSON("/ui/api/images/groups");
    state.groups = payload.groups || [];
    renderGroups();
  }

  async function loadAssets() {
    const payload = await getJSON("/ui/api/images/assets");
    state.assets = payload.assets || [];
    state.fitConfig = fitConfigForAssets(state.assets);
    updateSummary();
  }

  async function loadGroupDetail(groupId = state.activeGroupId) {
    if (!groupId) {
      state.activeGroupId = "";
      state.activeGroupImageIds = new Set();
      elements.imagesGroupDetail.textContent = "";
      renderGroups();
      updateMarkerStyles();
      updateSummary();
      return;
    }
    const payload = await getJSON(`/ui/api/images/groups/${groupId}`);
    state.activeGroupId = payload.id;
    state.activeGroupImageIds = new Set(payload.image_ids || []);
    elements.imagesGroupDetail.textContent = prettyJSON(payload);
    elements.imagesGroupName.value = payload.name || "";
    elements.imagesGroupDescription.value = payload.description || "";
    renderGroups();
    updateMarkerStyles();
    updateSummary();
  }

  function renderMap() {
    if (!mapIsVisible()) {
      return;
    }
    const map = ensureMap();
    map.invalidateSize(false);
    renderMarkers();
    if (!state.initialFitDone) {
      state.initialFitDone = true;
      fitToAssets();
    } else {
      updateSummary();
    }
  }

  function scheduleRender() {
    if (state.renderScheduled) {
      return;
    }
    state.renderScheduled = true;
    window.requestAnimationFrame(() => {
      state.renderScheduled = false;
      try {
        renderMap();
      } catch (error) {
        reportMapError(error);
      }
    });
  }

  async function refresh() {
    setImagesBadge("running", "LOADING");
    await Promise.all([loadGroups(), loadAssets()]);
    if (state.activeGroupId) {
      await loadGroupDetail(state.activeGroupId);
    } else {
      updateSummary();
    }
    scheduleRender();
    setImagesBadge("done", "READY");
  }

  async function createGroupFromSelection() {
    const name = elements.imagesGroupName.value.trim();
    if (!name) {
      throw new Error("group name is required");
    }
    const description = elements.imagesGroupDescription.value.trim() || null;
    const payload = await postJSON("/ui/api/images/groups", {
      name,
      description,
      image_ids: uniqueSorted([...state.selectedImageIds]),
    });
    state.activeGroupId = payload.id;
    await refresh();
    hooks.setStatus(`Created image group ${payload.name}`, "done");
  }

  async function deleteActiveGroup() {
    if (!state.activeGroupId) {
      throw new Error("select an image group first");
    }
    const groupName = activeGroup()?.name || state.activeGroupId;
    await deleteJSON(`/ui/api/images/groups/${state.activeGroupId}`);
    state.activeGroupId = "";
    state.activeGroupImageIds = new Set();
    await refresh();
    hooks.setStatus(`Deleted image group ${groupName}`, "done");
  }

  async function updateMembership(mode) {
    if (!state.activeGroupId) {
      throw new Error("select an image group first");
    }
    if (!state.selectedImageIds.size && mode !== "replace") {
      throw new Error("select one or more mapped images first");
    }
    await postJSON(`/ui/api/images/groups/${state.activeGroupId}/membership`, {
      mode,
      image_ids: uniqueSorted([...state.selectedImageIds]),
    });
    await refresh();
    hooks.setStatus(`${mode} group membership complete`, "done");
  }

  elements.imagesMapSurface.addEventListener(
    "pointerdown",
    (event) => {
      if (event.button !== 0 || !event.shiftKey || !state.map) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      state.drag = {
        startX: event.clientX,
        startY: event.clientY,
        panWasEnabled: state.map.dragging.enabled(),
      };
      if (state.drag.panWasEnabled) {
        state.map.dragging.disable();
      }
      elements.imagesMapSurface.setPointerCapture(event.pointerId);
      hideSelectionRect();
    },
    true,
  );

  elements.imagesMapSurface.addEventListener(
    "pointermove",
    (event) => {
      if (!state.drag) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      showSelectionRect(selectionBounds(event));
    },
    true,
  );

  elements.imagesMapSurface.addEventListener(
    "pointerup",
    (event) => {
      if (!state.drag) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      const bounds = selectionBounds(event);
      const selected = selectedIdsFromBounds(bounds);
      if (state.drag.panWasEnabled) {
        state.map.dragging.enable();
      }
      state.drag = null;
      hideSelectionRect();
      applySelection(selected);
    },
    true,
  );

  elements.imagesMapSurface.addEventListener(
    "pointercancel",
    (event) => {
      if (!state.drag) {
        return;
      }
      event.preventDefault();
      event.stopPropagation();
      if (state.drag.panWasEnabled) {
        state.map.dragging.enable();
      }
      state.drag = null;
      hideSelectionRect();
    },
    true,
  );

  elements.imagesRefreshButton.addEventListener("click", () => {
    refresh().catch((error) => hooks.setStatus(String(error), "error"));
  });
  elements.imagesCreateGroupButton.addEventListener("click", () => {
    createGroupFromSelection().catch((error) => hooks.setStatus(String(error), "error"));
  });
  elements.imagesDeleteGroupButton.addEventListener("click", () => {
    deleteActiveGroup().catch((error) => hooks.setStatus(String(error), "error"));
  });
  elements.imagesSelectionClearButton.addEventListener("click", () => {
    state.selectedImageIds = new Set();
    updateMarkerStyles();
    updateSummary();
  });
  elements.imagesFitButton.addEventListener("click", () => {
    try {
      fitToAssets();
    } catch (error) {
      reportMapError(error);
    }
  });
  elements.imagesGroupAddButton.addEventListener("click", () => {
    updateMembership("add").catch((error) => hooks.setStatus(String(error), "error"));
  });
  elements.imagesGroupRemoveButton.addEventListener("click", () => {
    updateMembership("remove").catch((error) => hooks.setStatus(String(error), "error"));
  });
  elements.imagesGroupReplaceButton.addEventListener("click", () => {
    updateMembership("replace").catch((error) => hooks.setStatus(String(error), "error"));
  });

  return {
    refresh,
    activate() {
      if (state.map) {
        state.map.invalidateSize(false);
      }
      scheduleRender();
      updateSummary();
    },
  };
}
