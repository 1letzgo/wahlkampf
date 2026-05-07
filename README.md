# VorOrt — Ortsverbands App

Mandantenfähige **FastAPI**-Webapp für die Organisation auf Oortsvereins-Ebene: **Termine** (inkl. Fraktion), **Kalender-Feeds** (ICS), **Sharepic-Generator**, **Plakat-Karte** und zentrale **OV-/Nutzerverwaltung**. Pro Ortsverband (OV) gibt es eigene Daten unter konfigurierbarem Speicherpfad (SQLite + Datei-Uploads).

---


## URLs und Mandanten

Die App ist erreichbar unter: https://vorort.spd-wst.de

---

## Anmeldung und Rechte

1. **Registrierung:** Unter **https://vorort.spd-wst.de/registrierung** könnt ihr euch registrieren.
2. **OV-Zugehörigkeit:** Bitte wählt dort Euren Heimat Ortsverband aus.
3. Der Administrator des OV kriegt eine Benachristigung und schaltet Euch frei.
4. Nach Freischaltung könnt Ihr die App dann nutzen.
5. Wer in mehr als einem OV oder auch im Kreis mitarbeitet, kann unter mein Profil weitere Verbände anfragen. Auch dies wird dann jeweils vom Administrator des Verbandes freigegeben.

---

## Termine

### Normale Termine anlegen

1. Meldet euch an und öffnet **Termine**.
2. **Neuer Termin** (sichtbar, wenn ihr im jeweiligen OV berechtigt seid).
3. Formular:
   - **Titel, Datum, Beginn** (Format `HH:MM`), optional **Ende**, **Ort**, **Beschreibung**.
   - **Link (optional):** z. B. RIS oder Videokonferenz — wird in Liste und Detailseite verlinkt und landet im ICS als URL.
   - **Externe Gäste:** Auswahlfelder (nur bei **normalen** Terminen, nicht bei Fraktion).
   - **Foto** und/oder **Sharepic als Terminbild** (wenn Sharepic für den OV aktiviert ist — siehe unten).
   - **Dateianhänge:** mehrere Dateien, Größe begrenzt (`MAX_UPLOAD_MB` / Anhänge-Limit im Code).

**Kreis / „für alle OVs“:** Wenn ein **Kreis-OV** per Umgebungsvariable `WAHKAMPF_KREIS_OV_SLUG` konfiguriert ist und ihr als **Kreis-Admin** im Kreis-Mandanten arbeitet, erscheint die Option **„In allen Ortsverbänden anzeigen“**. Diese Termine erscheinen dann auch in den anderen OVs (sichtbar wie vom Kreis beworben).

### Teilnahme, Kommentare, Bearbeiten

- Auf der **Terminliste** und der **Detailseite** könnt ihr **zusagen** oder **absagen** (sofern der Termin noch nicht vorbei ist).
- **Kommentare** und die Teilnehmerliste sind pro Termin sichtbar.
- **Bearbeiten/Löschen:** Nur für Ersteller, OV-Admins oder entsprechend berechtigte Konten (grenzüberschreitend für Kreis-Termine nach den Regeln im Code).

---

## Fraktion — Besonderheiten

Die Fraktionsfunktion ist pro OV **deaktivierbar** (Superadmin: Feature **„Fraktion“**). Wenn sie an ist:

### Wer darf was?

- **Fraktionstermine anlegen** dürfen nur **Fraktionsmitglieder** (und Superadmins). Das Flag **„Fraktionsmitglied“** setzt der OV-Admin bei der Mitgliedschaft eines Nutzers.
- Die Oberfläche liegt unter **`/m/<slug>/fraktion/termine`** (Menüpunkt bzw. Tab „Fraktion“ in der Terminübersicht).

### Unterschiede zu normalen Terminen

- Es gibt **keine** Sektion **„Externe Gäste“** bei Fraktionsterminen.
- **Vertraulichkeit:** Beim Anlegen/Bearbeiten könnt ihr **„Vertraulich — nur für Fraktionsmitglieder sichtbar“** aktivieren.
  - **Ohne** diesen Haken sehen **alle freigegebenen Verbandsmitglieder** den Termin.
  - **Mit** Haken sehen nur **Fraktionsmitglieder** (und Superadmin) den Termin — in der Web-UI und in **Kalender-Feeds** (siehe unten: vertrauliche Termine fehlen in öffentlichen Feeds ohne passende Identität).

### Sharepic direkt im Terminformular

Wenn Sharepic aktiv ist, könnt ihr im Terminformular ein **Sharepic erzeugen und als Terminfoto setzen** (wird vor dem Speichern als JPEG in das Foto-Feld übernommen). Datum/Zeit, Titel und Ort werden aus dem Formular übernommen.


### Persönliche Feeds (eingeloggt)

Auf der **Terminliste** (und analog in der Termin-Detailansicht) gibt es **„Abonnieren“**. Es werden zwei Feeds angeboten:

| Feed | Inhalt |
|------|--------|
| **Zugesagt** | Termine, für die ihr **zugesagt** habt — über alle OVs, in denen ihr Mitglied seid (plus Kreis-Termine „für alle OVs“ nach derselben Logik wie in der App). |
| **ALLE** | **Alle** Termine in euren freigegebenen Verbänden — mit derselben Filterlogik für **vertrauliche Fraktionstermine** (nur sichtbar, wenn ihr Fraktionsmitglied im jeweiligen OV seid). |

- Links **Apple** nutzen `webcal://…` (Kalender-App).
- **Google** öffnet die Google-Kalender-Einstellung „per URL hinzufügen“.
- **Kopieren** legt die **HTTPS-**ICS-Adresse in die Zwischenablage — für **Microsoft Outlook**, **Outlook im Web** oder **Kalender in Microsoft 365**: dort „Kalender abonnieren“ / „Abonnement von Web“ / „Aus dem Internet“ und die URL einfügen.

Die URLs enthalten einen **geheimen Token** (`t=…`), der eurem Konto zugeordnet ist. **Nicht öffentlich teilen** — wer die URL kennt, sieht den entsprechenden Feed.

---

## Sharepic-Generator

Menüpunkt **„Sharepic“** (`/m/<slug>/sharepic`), sofern das Feature für den OV **nicht abgeschaltet** ist.

- **Format:** 768×1024 Pixel, mit **SPD-Maske** (Logo, roter Balken, Fußzeile).
- **Foto:** eigenes Bild oder **Hintergrundvorlage** (vom Superadmin pro OV hochladbar, begrenzte Anzahl Vorlagen).
- **Texte:** Slogan (oben rechts), Kurztext im Mittelbalken (Zeichenlimit), Text unten; **OV-Anzeigename** unter dem Schriftzug.
- **Bedienung:** Foto verschieben und zoomen, **Speichern** lädt die Grafik als Datei herunter; **Teilen** nutzt die native Share-Funktion des Browsers, falls verfügbar.

**Standard-Slogan** (z. B. „Für … Für Dich.“) kann der Superadmin pro OV setzen.



---

## Plakate

Menüpunkt **„Plakate“** (`/m/<slug>/plakate`), sofern aktiviert.

- **Karte** (OpenStreetMap / Leaflet) mit allen **aktuell hängenden** Plakat-Meldungen.
- **Neues Plakat:** Standort durch **Tippen auf die Karte**, **„Neues Plakat am aktuellen Standort“** (wenn der Browser Standortfreigabe hat) oder **„Mein Standort“** zur Orientierung.
- Pro Meldung optional **Notiz** und **Foto** (JPEG/PNG/WebP, Größe begrenzt).
- **„Abhängen“** markiert einen Eintrag als entfernt (für alle sichtbar in der Historie/Logik der App; genaue Darstellung siehe UI).

Superadmins können die Plakat-Daten eines OV bei Bedarf **komplett löschen** (Wartung).

---

## Lizenz / Projekt

Internes Wahlkampf-Projekt — bei Fragen zur Installation oder zu Hosting/Firewall (z. B. ausgehende HTTP-Abrufe für Fraktions-Kalender) den Betrieb der Instanz konsultieren.
