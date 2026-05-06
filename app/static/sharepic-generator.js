/**
 * Sharepic-Rendering (768×1024) für Einbindung außerhalb der Sharepic-Seite (z. B. Terminformular).
 * Logik synchron zu sharepic.html (Maske, Cover-Hintergrund, Texte).
 */
(function () {
  var W = 768;
  var H = 1024;
  var MASK_REF_W = 1080;
  var MASK_REF_H = 1350;
  var MASK_HEADER_END_Y = 188;
  var MASK_MID_BAR_TOP_Y = 239;
  var MASK_MID_BAR_LAST_RED_ROW = 352;
  var MASK_PHOTO_START_Y = MASK_HEADER_END_Y;
  var MASK_FOOTER_TOP_Y = 1162;
  var MASK_SPD_P_CENTER_X = 190.07;
  var MASK_OV_TEXT_TOP_Y = 114;

  var SLOGAN_MAX_LINES = 3;
  var SLOGAN_RIGHT_PAD = 8;
  var SLOGAN_MAX_WIDTH_FRAC = 0.62;
  var MID_MAX_LINES = 1;
  var MID_CHAR_LIMIT = 30;
  var TEXT2_LH = 50;
  var TEXT2_MAX_W = 680;
  var TEXT2_MAX_LINES = 2;
  var FONT_FOOT = '700 50px "Open Sans", system-ui, sans-serif';
  var FILL = "#ffffff";

  function loadImage(src, useAnonymousCors) {
    return new Promise(function (resolve, reject) {
      var im = new Image();
      if (useAnonymousCors) im.crossOrigin = "anonymous";
      im.onload = function () {
        resolve(im);
      };
      im.onerror = function () {
        reject(new Error("Bild konnte nicht geladen werden: " + src));
      };
      im.src = src;
    });
  }

  function buildLayout(maskImg) {
    var nh = maskImg.naturalHeight || MASK_REF_H;
    var nw = maskImg.naturalWidth || MASK_REF_W;
    var sy = H / nh;
    var sx = W / nw;
    var headerBottom = Math.round(MASK_HEADER_END_Y * sy);
    var photoTop = Math.round(MASK_PHOTO_START_Y * sy);
    var photoBottom = Math.round(MASK_FOOTER_TOP_Y * sy);
    var ovTextTop = Math.round(MASK_OV_TEXT_TOP_Y * sy);
    var spdPCenterX = MASK_SPD_P_CENTER_X * sx;
    var midBarCenterY = ((MASK_MID_BAR_TOP_Y + MASK_MID_BAR_LAST_RED_ROW) / 2) * sy;
    return {
      headerBottom: headerBottom,
      midBarTop: Math.round(MASK_MID_BAR_TOP_Y * sy),
      midBarCenterY: midBarCenterY,
      ovTextTop: ovTextTop,
      spdPCenterX: spdPCenterX,
      footerTop: photoBottom,
      footerH: H - photoBottom,
      photoRect: { x: 0, y: photoTop, w: W, h: photoBottom - photoTop },
    };
  }

  function photoCoverScale(img, rw, rh, zoom) {
    var iw = img.naturalWidth;
    var ih = img.naturalHeight;
    var scale0 = Math.max(rw / iw, rh / ih);
    return {
      dw: iw * scale0 * zoom,
      dh: ih * scale0 * zoom,
    };
  }

  function drawCoverPanZoom(ctx, img, rx, ry, rw, rh, zoom, panX, panY) {
    if (!img || !img.naturalWidth || !img.naturalHeight) return;
    var iw = img.naturalWidth;
    var ih = img.naturalHeight;
    var dims = photoCoverScale(img, rw, rh, zoom);
    var dw = dims.dw;
    var dh = dims.dh;
    var cx = rx + rw * 0.5 + panX;
    var cy = ry + rh * 0.5 + panY;
    var dx = cx - dw * 0.5;
    var dy = cy - dh * 0.5;
    ctx.save();
    ctx.beginPath();
    ctx.rect(rx, ry, rw, rh);
    ctx.clip();
    ctx.drawImage(img, 0, 0, iw, ih, dx, dy, dw, dh);
    ctx.restore();
  }

  function wrapParagraph(ctx, text, maxW) {
    if (!text) return [];
    var words = text.split(/\s+/).filter(Boolean);
    var lines = [];
    var line = "";
    for (var i = 0; i < words.length; i++) {
      var w = words[i];
      var test = line ? line + " " + w : w;
      if (ctx.measureText(test).width <= maxW) {
        line = test;
      } else {
        if (line) lines.push(line);
        if (ctx.measureText(w).width <= maxW) {
          line = w;
        } else {
          var rest = w;
          while (rest.length) {
            var lo = 1;
            var hi = rest.length;
            while (lo < hi) {
              var mid = Math.ceil((lo + hi) / 2);
              if (ctx.measureText(rest.slice(0, mid)).width <= maxW) lo = mid;
              else hi = mid - 1;
            }
            if (lo < 1) lo = 1;
            lines.push(rest.slice(0, lo));
            rest = rest.slice(lo);
          }
          line = "";
        }
      }
    }
    if (line) lines.push(line);
    return lines;
  }

  function textToLines(ctx, raw, maxW, maxLines) {
    var paras = raw.split(/\n/);
    var out = [];
    for (var p = 0; p < paras.length && out.length < maxLines; p++) {
      var wrapped = wrapParagraph(ctx, paras[p].trim(), maxW);
      for (var j = 0; j < wrapped.length && out.length < maxLines; j++) {
        out.push(wrapped[j]);
      }
    }
    return out.slice(0, maxLines);
  }

  function drawOvNameUnderLogo(ctx, L, ovDisplayName) {
    var name = (ovDisplayName && String(ovDisplayName).trim()) || "";
    if (!name) return;
    var anchorX = L.spdPCenterX;
    var maxHalf = Math.min(anchorX - 14, W * 0.48 - anchorX - 6);
    var maxW = Math.max(100, maxHalf * 2);
    ctx.fillStyle = FILL;
    ctx.textAlign = "center";
    ctx.textBaseline = "top";
    var fs = 26;
    var lh;
    var lines;
    var y0 = L.ovTextTop - 5;
    var headerAvail = L.headerBottom - y0 - 8;
    for (; fs >= 15; fs -= 2) {
      lh = Math.round(fs * 1.14);
      ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
      lines = textToLines(ctx, name, maxW, 2);
      var blockH = lines.length * lh;
      var ok = blockH <= headerAvail && blockH <= L.headerBottom - y0 - 4;
      if (ok) {
        for (var w = 0; w < lines.length && ok; w++) {
          if (ctx.measureText(lines[w]).width > maxW) ok = false;
        }
      }
      if (ok) break;
    }
    lh = Math.round(fs * 1.14);
    ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
    lines = textToLines(ctx, name, maxW, 2);
    if (!lines.length) return;
    for (var i = 0; i < lines.length; i++) {
      ctx.fillText(lines[i], anchorX, y0 + i * lh);
    }
    ctx.textAlign = "left";
  }

  function drawMiddleBanner(ctx, L, rawMid) {
    var raw = rawMid || "";
    if (raw.length > MID_CHAR_LIMIT) raw = raw.slice(0, MID_CHAR_LIMIT);
    var maxW = W - 48;
    var cx = W * 0.5;
    var cy = L.midBarCenterY;
    ctx.fillStyle = FILL;
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    var fs = 40;
    var lines;
    for (; fs >= 18; fs -= 2) {
      ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
      lines = textToLines(ctx, raw, maxW, MID_MAX_LINES);
      if (!lines.length) return;
      var ok = ctx.measureText(lines[0]).width <= maxW;
      if (ok) break;
    }
    ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
    lines = textToLines(ctx, raw, maxW, MID_MAX_LINES);
    if (!lines.length) {
      ctx.textAlign = "left";
      ctx.textBaseline = "top";
      return;
    }
    ctx.fillText(lines[0], cx, cy);
    ctx.textAlign = "left";
    ctx.textBaseline = "top";
  }

  function drawHeaderSlogan(ctx, L, sloganRaw) {
    var raw = sloganRaw || "";
    var maxW = W * SLOGAN_MAX_WIDTH_FRAC;
    var xRight = W - SLOGAN_RIGHT_PAD;
    ctx.fillStyle = FILL;
    ctx.textAlign = "right";
    ctx.textBaseline = "top";
    var fs = 46;
    var lh;
    var lines;
    for (; fs >= 22; fs -= 2) {
      lh = Math.round(fs * 1.1);
      ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
      lines = textToLines(ctx, raw, maxW, SLOGAN_MAX_LINES);
      var blockH = lines.length * lh;
      var ok = blockH <= L.headerBottom - 10;
      if (ok) {
        for (var w = 0; w < lines.length && ok; w++) {
          if (ctx.measureText(lines[w]).width > maxW) ok = false;
        }
      }
      if (ok) break;
    }
    lh = Math.round(fs * 1.1);
    ctx.font = '700 ' + fs + 'px "Open Sans", system-ui, sans-serif';
    lines = textToLines(ctx, raw, maxW, SLOGAN_MAX_LINES);
    var blockH = lines.length * lh;
    var y0 = Math.max(4, Math.round((L.headerBottom - blockH) / 2) - 10);
    for (var i = 0; i < lines.length; i++) {
      ctx.fillText(lines[i], xRight, y0 + i * lh);
    }
    ctx.textAlign = "left";
  }

  function drawFooterText(ctx, L, text2Raw) {
    var t2 = text2Raw || "";
    ctx.font = FONT_FOOT;
    ctx.fillStyle = FILL;
    ctx.textBaseline = "top";
    var lines2 = textToLines(ctx, t2, TEXT2_MAX_W, TEXT2_MAX_LINES);
    ctx.textAlign = "center";
    var cx2 = W * 0.5;
    var block2 = lines2.length * TEXT2_LH;
    var y2 = L.footerTop + Math.floor((L.footerH - block2) / 2) - 5;
    for (var j = 0; j < lines2.length; j++) {
      ctx.fillText(lines2[j], cx2, y2 + j * TEXT2_LH);
    }
    ctx.textAlign = "left";
  }

  async function preloadFonts() {
    if (!document.fonts || !document.fonts.load) return;
    try {
      await document.fonts.load(FONT_FOOT);
      await document.fonts.load('700 40px "Open Sans", system-ui, sans-serif');
      await document.fonts.load('700 26px "Open Sans", system-ui, sans-serif');
      await document.fonts.load('700 46px "Open Sans", system-ui, sans-serif');
    } catch (e) {}
  }

  /**
   * @param {object} opts
   * @param {string} opts.maskSrc — vollständige URL zur Maske
   * @param {string} opts.ovDisplayName
   * @param {string} opts.slogan — mehrzeilig mit \\n möglich
   * @param {string} opts.midText — mittlerer Balken (max. 30 Zeichen werden verwendet)
   * @param {string} opts.text2 — Fußtext (max. 2 Zeilen, \\n)
   * @param {string|null} opts.backgroundImageUrl — optional, ohne CORS (gleicher Ursprung)
   * @param {number} [opts.jpegQuality]
   */
  async function renderToBlob(opts) {
    var maskSrc = opts.maskSrc;
    var ovDisplayName = opts.ovDisplayName || "";
    var slogan = opts.slogan || "";
    var midText = opts.midText || "";
    if (midText.length > MID_CHAR_LIMIT) midText = midText.slice(0, MID_CHAR_LIMIT);
    var text2 = opts.text2 || "";
    var bgUrl = opts.backgroundImageUrl || "";
    var jpegQ = typeof opts.jpegQuality === "number" ? opts.jpegQuality : 0.92;

    var maskImg = await loadImage(maskSrc, false);
    await preloadFonts();

    var bgImg = null;
    if (bgUrl) {
      try {
        bgImg = await loadImage(bgUrl, false);
      } catch (e) {
        bgImg = null;
      }
    }

    var L = buildLayout(maskImg);
    var canvas = document.createElement("canvas");
    canvas.width = W;
    canvas.height = H;
    var ctx = canvas.getContext("2d");

    ctx.fillStyle = "#e8e4dc";
    ctx.fillRect(0, 0, W, H);
    if (bgImg && bgImg.complete && bgImg.naturalWidth) {
      var P = L.photoRect;
      drawCoverPanZoom(ctx, bgImg, P.x, P.y, P.w, P.h, 1, 0, 0);
    }
    ctx.drawImage(maskImg, 0, 0, W, H);
    drawOvNameUnderLogo(ctx, L, ovDisplayName);
    drawHeaderSlogan(ctx, L, slogan);
    drawMiddleBanner(ctx, L, midText);
    drawFooterText(ctx, L, text2);

    return new Promise(function (resolve, reject) {
      canvas.toBlob(
        function (blob) {
          if (!blob) reject(new Error("JPEG konnte nicht erzeugt werden."));
          else resolve(blob);
        },
        "image/jpeg",
        jpegQ
      );
    });
  }

  window.WahlkampfSharepicGenerator = {
    renderToBlob: renderToBlob,
    MID_CHAR_LIMIT: MID_CHAR_LIMIT,
  };
})();
