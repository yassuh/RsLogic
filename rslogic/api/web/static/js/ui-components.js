function appendChildren(node, children) {
  for (const child of children) {
    if (child == null) {
      continue;
    }
    if (typeof child === "string") {
      node.append(document.createTextNode(child));
      continue;
    }
    node.append(child);
  }
  return node;
}

export function element(tagName, options = {}) {
  const node =
    options.namespace === "svg"
      ? document.createElementNS("http://www.w3.org/2000/svg", tagName)
      : document.createElement(tagName);
  if (options.id) {
    node.id = options.id;
  }
  if (options.className) {
    node.className = options.className;
  }
  if (options.type) {
    node.type = options.type;
  }
  if (options.text != null) {
    node.textContent = String(options.text);
  }
  if (options.html != null) {
    node.innerHTML = options.html;
  }
  if (options.value != null) {
    node.value = options.value;
  }
  if (options.checked != null) {
    node.checked = Boolean(options.checked);
  }
  if (options.spellcheck != null) {
    node.spellcheck = Boolean(options.spellcheck);
  }
  if (options.attrs) {
    for (const [key, value] of Object.entries(options.attrs)) {
      if (value != null) {
        node.setAttribute(key, String(value));
      }
    }
  }
  if (options.dataset) {
    for (const [key, value] of Object.entries(options.dataset)) {
      if (value != null) {
        node.dataset[key] = String(value);
      }
    }
  }
  if (options.children) {
    appendChildren(node, options.children);
  }
  return node;
}

export function button(options = {}) {
  return element("button", {
    ...options,
    type: options.type || "button",
    text: options.label ?? options.text,
  });
}

export function input(options = {}) {
  return element("input", {
    ...options,
    type: options.type || "text",
  });
}

export function textarea(options = {}) {
  return element("textarea", options);
}

export function select(options = {}) {
  const node = element("select", options);
  if (Array.isArray(options.options)) {
    for (const item of options.options) {
      const option = element("option", {
        value: item.value,
        text: item.label,
        attrs: item.selected ? { selected: "selected" } : undefined,
      });
      node.append(option);
    }
  }
  return node;
}

export function fieldRow({ className = "field-row", label, control }) {
  return element("label", {
    className,
    children: [
      element("span", { className: "field-label", text: label }),
      control,
    ],
  });
}

export function fieldBlock({ label, control, className = "field-block" }) {
  return element("label", {
    className,
    children: [
      element("span", { className: "field-label", text: label }),
      control,
    ],
  });
}

export function toolbar(children, className = "toolbar") {
  return element("div", { className, children });
}

export function progressTrack(id) {
  return element("div", {
    id,
    className: "progress-track",
    children: [element("div", { className: "progress-fill" })],
  });
}

export function listPanel(id, className = "list-panel") {
  return element("div", { id, className });
}

export function consolePanel(id, className = "console-panel") {
  return element("pre", { id, className });
}

export function subHeader(text) {
  return element("div", { className: "sub-header", text });
}

export function stateBadge(id, label, status = "idle") {
  return element("span", { id, className: `state-badge state-${status}`, text: label });
}

export function tile({ className, title, badge, body }) {
  return element("section", {
    className: `tile ${className}`,
    children: [
      element("div", {
        className: "tile-header",
        children: [element("span", { text: title }), badge],
      }),
      element("div", { className: "tile-body", children: Array.isArray(body) ? body : [body] }),
    ],
  });
}

export function ribbon(blocks, className) {
  return element("header", { className, children: blocks });
}

export function ribbonBlock(children, className = "ribbon-block") {
  return element("div", { className, children });
}

export function textStack(primary, secondary, className, metaClassName) {
  return element("div", {
    className,
    children: [
      element("span", { text: primary }),
      element("span", { className: metaClassName, text: secondary }),
    ],
  });
}

export function listRowButton({ className, selected = false, children = [] }) {
  return button({
    className: `${className}${selected ? " is-selected" : ""}`,
    children,
  });
}

export function moveStack({ upDisabled = false, downDisabled = false, onUp, onDown }) {
  const upButton = button({ className: "step-move-button", text: "↑" });
  upButton.disabled = upDisabled;
  if (typeof onUp === "function") {
    upButton.addEventListener("click", onUp);
  }

  const downButton = button({ className: "step-move-button", text: "↓" });
  downButton.disabled = downDisabled;
  if (typeof onDown === "function") {
    downButton.addEventListener("click", onDown);
  }

  return element("div", {
    className: "step-move-stack",
    children: [upButton, downButton],
  });
}

export function workflowButton({ id, label, workflow }) {
  return button({
    id,
    className: "workflow-ribbon-button",
    text: label,
    dataset: { workflow },
  });
}

export function workflowView({ className, workflow, children }) {
  return element("section", {
    className: `workflow-view ${className}`,
    dataset: { workflow },
    children,
  });
}
