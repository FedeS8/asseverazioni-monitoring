#!/usr/bin/env python3
"""
Script per monitoraggio asseverazioni in stato Parziale
Invia reminder settimanali con azioni consigliate
"""

import os
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Tuple
import logging

# Configurazione logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsseverazioniReminderManager:
    def __init__(self):
        # Configurazione email Gmail (sicura per test)
        self.email_mittente = os.getenv('EMAIL_MITTENTE', 'federica.pazzolasatta@gmail.com')
        self.password_email = os.getenv('PASSWORD_EMAIL', 'ucbg rykk jptm xgyn')
        self.email_destinatari = os.getenv('EMAIL_DESTINATARI', 'f.satta@innovazione.gov.it').split(',')
        
        # Configurazione SMTP Gmail
        self.smtp_server = 'smtp.gmail.com'
        self.smtp_port = 587
        
        # Configurazione SharePoint - COMMENTATA per sicurezza
        # self.sharepoint_url = os.getenv('SHAREPOINT_URL')
        
        # Validazione configurazione
        if not all([self.email_mittente, self.password_email, self.email_destinatari[0]]):
            raise ValueError("Configurazione email incompleta. Verificare le variabili d'ambiente.")

    def load_csv_data(self, csv_file_path: str) -> pd.DataFrame:
        """Carica i dati dal file CSV con gestione automatica encoding e separatori"""
        try:
            logger.info(f"Caricamento file CSV: {csv_file_path}")
            
            # Prova diverse combinazioni di encoding e separatori
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            separators = [',', ';', '\t', '|']
            
            df = None
            successful_config = None
            
            for encoding in encodings:
                for sep in separators:
                    try:
                        df_temp = pd.read_csv(csv_file_path, encoding=encoding, sep=sep)
                        
                        # Verifica che il CSV sia valido
                        if (len(df_temp) > 0 and 
                            len(df_temp.columns) >= 5 and  # Almeno 5 colonne
                            not df_temp.columns[0].startswith('Unnamed')):  # Non colonne senza nome
                            
                            df = df_temp
                            successful_config = f"encoding={encoding}, separator='{sep}'"
                            logger.info(f"✅ CSV caricato con {successful_config}: {len(df)} righe, {len(df.columns)} colonne")
                            break
                    except Exception as e:
                        logger.debug(f"Tentativo encoding={encoding}, sep='{sep}' fallito: {str(e)[:100]}")
                        continue
                
                if df is not None and len(df) > 0:
                    break
            
            if df is None or len(df) == 0:
                raise ValueError("Impossibile caricare il file CSV con nessuna combinazione di encoding/separatore")
            
            logger.info(f"📊 CSV caricato con successo usando {successful_config}")
            logger.info(f"Dimensioni: {df.shape[0]} righe, {df.shape[1]} colonne")
            logger.info(f"Colonne: {list(df.columns)}")
            logger.info(f"Primo record:\n{df.head(1).to_string()}")
            
            # Pulizia colonne
            df.columns = [str(col).strip() for col in df.columns]
            
            # Rimuovi righe completamente vuote
            df = df.dropna(how='all')
            logger.info(f"Dopo pulizia righe vuote: {len(df)} righe")
            
            # Verifica presenza colonne essenziali
            required_columns = [
                'Nome ente', 'Funding Request Name', 'Oggetto', 
                'Data ultima assegnazione', 'L\'asseverazione è bloccata?',
                'Ultimo esito asseverazione tecnica', 'Stato progetto'
            ]
            
            # Matching colonne con tolleranza
            column_mapping = {}
            for req_col in required_columns:
                found = False
                for actual_col in df.columns:
                    # Pulizia per matching
                    req_clean = req_col.lower().replace(' ', '').replace('\'', '').replace('?', '')
                    actual_clean = str(actual_col).lower().replace(' ', '').replace('\'', '').replace('?', '')
                    
                    if (req_clean == actual_clean or 
                        req_clean in actual_clean or 
                        actual_clean in req_clean):
                        column_mapping[actual_col] = req_col
                        found = True
                        break
                
                if not found:
                    logger.warning(f"Colonna non trovata: {req_col}")
            
            # Applica mapping
            if column_mapping:
                df = df.rename(columns=column_mapping)
                logger.info(f"Colonne rinominate: {column_mapping}")
            
            # Verifica colonne finali
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"COLONNE MANCANTI: {missing_columns}")
                logger.error(f"COLONNE DISPONIBILI: {list(df.columns)}")
                raise ValueError(f"Colonne essenziali mancanti: {missing_columns}")
            
            logger.info("✅ Tutte le colonne essenziali sono presenti!")
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento CSV: {e}")
            raise
    
    def convert_sharepoint_url_to_download(self, sharepoint_url: str) -> str:
        """Converte un link di condivisione SharePoint in URL di download diretto"""
        try:
            # Metodo 1: Conversione link di condivisione in download diretto
            if '/:x:/g/personal/' in sharepoint_url:
                # Estrae i parametri dal link
                import urllib.parse as urlparse
                parsed = urlparse.urlparse(sharepoint_url)
                
                # Costruisce URL di download Microsoft
                if 'sharepoint.com' in sharepoint_url:
                    # Prova formato download diretto
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    
                    # Estrae l'ID del documento
                    import re
                    doc_match = re.search(r'/([A-Za-z0-9_-]{20,})/', sharepoint_url)
                    if doc_match:
                        doc_id = doc_match.group(1)
                        
                        # Prova diversi formati di download
                        download_formats = [
                            f"{base_url}/_layouts/15/download.aspx?share={doc_id}",
                            f"{sharepoint_url}&download=1",
                            f"{sharepoint_url.split('?')[0]}?download=1",
                            sharepoint_url.replace('/:x:/', '/_layouts/15/download.aspx?SourceUrl=')
                        ]
                        
                        # Restituisce il primo formato
                        return download_formats[1]  # Prova con &download=1
            
            # Fallback: aggiunge parametro download
            if '?' in sharepoint_url:
                return f"{sharepoint_url}&download=1"
            else:
                return f"{sharepoint_url}?download=1"
            
        except Exception as e:
            logger.warning(f"Errore nella conversione URL SharePoint: {e}")
            return sharepoint_url

    def download_excel_from_sharepoint(self, sharepoint_url: str) -> str:
        """METODO COMMENTATO - Scarica il file Excel da SharePoint con multiple strategie"""
        # FUNZIONALITÀ DISABILITATA PER SICUREZZA
        logger.warning("🚫 Download SharePoint disabilitato per sicurezza")
        logger.info("💡 Usa file locale CSV: data/asseverazioni.csv")
        raise Exception("Download SharePoint non disponibile - usa file locale")
        
        # CODICE ORIGINALE COMMENTATO:
        # try:
        #     import requests
        #     logger.info(f"Tentativo di download da SharePoint...")
        #     # ... resto del codice SharePoint commentato per sicurezza
        # except Exception as e:
        #     logger.error(f"Errore nel download da SharePoint: {e}")
        #     raise

    def load_excel_data(self, file_path: str = None, sharepoint_url: str = None) -> pd.DataFrame:
        """Carica i dati dal file Excel (locale o SharePoint)"""
        try:
            # Se fornito URL SharePoint, scarica il file
            if sharepoint_url:
                file_path = self.download_excel_from_sharepoint(sharepoint_url)
            elif not file_path:
                raise ValueError("Fornire file_path o sharepoint_url")
            
            # Carica il file Excel
            df = pd.read_excel(file_path, engine='openpyxl')
            logger.info(f"Caricati {len(df)} record dal file Excel")
            
            # Standardizza i nomi delle colonne
            df.columns = df.columns.str.strip()
            
            # Log delle colonne disponibili per debugging
            logger.info(f"Colonne disponibili: {list(df.columns)}")
            
            # Verifica presenza colonne essenziali
            required_columns = [
                'Nome ente', 'Funding Request Name', 'Oggetto', 
                'Data ultima assegnazione', 'L\'asseverazione è bloccata?',
                'Ultimo esito asseverazione tecnica', 'Stato progetto'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.warning(f"Colonne mancanti: {missing_columns}")
                logger.info("Tentativo di matching fuzzy delle colonne...")
                
                # Prova matching fuzzy per gestire piccole differenze nei nomi
                column_mapping = {}
                for req_col in missing_columns:
                    for actual_col in df.columns:
                        if req_col.lower().replace(' ', '') in actual_col.lower().replace(' ', ''):
                            column_mapping[actual_col] = req_col
                            break
                
                # Rinomina le colonne trovate
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    logger.info(f"Colonne rinominate: {column_mapping}")
                    
                    # Ricontrolla colonne mancanti
                    missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Colonne mancanti dopo tentativo di matching: {missing_columns}")
            
            # Pulizia del file temporaneo se scaricato da SharePoint
            if sharepoint_url and file_path.startswith('temp_'):
                import os
                try:
                    os.remove(file_path)
                    logger.info("File temporaneo rimosso")
                except:
                    pass
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nel caricamento file Excel: {e}")
            raise
    
    def parse_date_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """Converte la colonna data in formato datetime"""
        try:
            # Gestisce celle vuote nella colonna data
            df['Data ultima assegnazione'] = df['Data ultima assegnazione'].fillna('')
            
            # Converte la data ultima assegnazione
            df['Data ultima assegnazione'] = pd.to_datetime(
                df['Data ultima assegnazione'], 
                format='%d/%m/%Y %H:%M',
                errors='coerce'  # Converte errori in NaT
            )
            
            # Calcola i giorni dalla data ultima assegnazione
            today = datetime.now()
            df['Giorni da ultima assegnazione'] = (today - df['Data ultima assegnazione']).dt.days
            
            # Gestisce valori NaN (date non valide)
            df['Giorni da ultima assegnazione'] = df['Giorni da ultima assegnazione'].fillna(0)
            
            logger.info("Date parsate con successo")
            
            # Debug: mostra alcune date per verifica
            valid_dates = df[df['Data ultima assegnazione'].notna()]
            if len(valid_dates) > 0:
                logger.info(f"Esempi di date parsate:")
                for i, row in valid_dates.head(3).iterrows():
                    logger.info(f"  {row['Nome ente']}: {row['Data ultima assegnazione']} ({row['Giorni da ultima assegnazione']} giorni fa)")
            
            return df
            
        except Exception as e:
            logger.error(f"Errore nel parsing delle date: {e}")
            raise
    
    def filter_partial_assessments(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filtra solo le asseverazioni con esito Parziale e rimuove duplicati per ente"""
        # Gestisce celle vuote nella colonna "Ultimo esito asseverazione tecnica"
        df['Ultimo esito asseverazione tecnica'] = df['Ultimo esito asseverazione tecnica'].fillna('')
        
        # Filtra solo i record con "Parziale" (ignora celle vuote)
        partial_df = df[df['Ultimo esito asseverazione tecnica'].str.strip() == 'Parziale'].copy()
        
        logger.info(f"Trovate {len(partial_df)} asseverazioni in stato Parziale su {len(df)} totali")
        
        if len(partial_df) == 0:
            logger.warning("ATTENZIONE: Nessuna asseverazione trovata con stato 'Parziale'")
            logger.info("Valori unici nella colonna 'Ultimo esito asseverazione tecnica':")
            unique_values = df['Ultimo esito asseverazione tecnica'].value_counts(dropna=False)
            for value, count in unique_values.items():
                logger.info(f"  '{value}': {count} occorrenze")
            return partial_df
        
        # NUOVA LOGICA: Rimuovi duplicati per ente, tenendo solo il più recente
        logger.info("Rimozione duplicati per ente (tenendo solo l'asseverazione più recente)...")
        
        # Converte la data per il sorting (se non già fatto)
        if 'Data ultima assegnazione' in partial_df.columns:
            partial_df['Data ultima assegnazione'] = pd.to_datetime(
                partial_df['Data ultima assegnazione'], 
                format='%d/%m/%Y %H:%M',
                errors='coerce'
            )
        
        # Raggruppa per ente e prende solo il record più recente
        # Ordina per data decrescente e prende il primo per ogni ente
        partial_df_sorted = partial_df.sort_values('Data ultima assegnazione', ascending=False)
        partial_df_unique = partial_df_sorted.groupby('Nome ente').first().reset_index()
        
        # Log dei duplicati rimossi
        duplicates_removed = len(partial_df) - len(partial_df_unique)
        if duplicates_removed > 0:
            logger.info(f"✅ Rimossi {duplicates_removed} duplicati")
            
            # Mostra quali enti avevano duplicati
            duplicate_entities = partial_df.groupby('Nome ente').size()
            duplicate_entities = duplicate_entities[duplicate_entities > 1]
            
            for ente, count in duplicate_entities.items():
                # Trova le date per questo ente
                ente_dates = partial_df[partial_df['Nome ente'] == ente]['Data ultima assegnazione'].sort_values(ascending=False)
                latest_date = ente_dates.iloc[0].strftime('%d/%m/%Y %H:%M')
                logger.info(f"  {ente}: {count} asseverazioni → tenuto solo {latest_date}")
        else:
            logger.info("✅ Nessun duplicato trovato")
        
        logger.info(f"Risultato finale: {len(partial_df_unique)} asseverazioni uniche in stato Parziale")
        
        return partial_df_unique
    
    def categorize_alerts(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Categorizza gli alert in base allo stato, giorni e misura PNRR"""
        alerts = {
            'ente_1_2_15_giorni': [],
            'ente_1_2_30_giorni': [],
            'ente_1_4_1_15_giorni': [],
            'ente_1_4_1_30_giorni': [],
            'verifica_1_2_15_giorni': [],
            'verifica_1_2_30_giorni': [],
            'verifica_1_4_1_15_giorni': [],
            'verifica_1_4_1_30_giorni': []
        }
        
        for _, row in df.iterrows():
            giorni = row['Giorni da ultima assegnazione']
            stato = row['Stato progetto']
            oggetto = row['Oggetto']
            
            # Determina la misura PNRR dall'oggetto
            if '1.2' in oggetto:
                misura = '1_2'
            elif '1.4.1' in oggetto:
                misura = '1_4_1'
            else:
                logger.warning(f"Misura non riconosciuta per: {row['Nome ente']} - {oggetto}")
                continue  # Salta se non riconosce la misura
            
            # Gestisce valori NaN nella colonna "L'asseverazione è bloccata?"
            is_blocked_value = row['L\'asseverazione è bloccata?']
            if pd.isna(is_blocked_value) or is_blocked_value == '':
                is_blocked = False
            else:
                is_blocked = str(is_blocked_value).strip().lower() == 'sì'
            
            # Salta record con date non valide
            if pd.isna(giorni) or giorni <= 0:
                logger.warning(f"Saltando record con data non valida: {row['Nome ente']}")
                continue
            
            # Crea oggetto alert
            alert_data = {
                'nome_ente': row['Nome ente'],
                'funding_request': row['Funding Request Name'],
                'oggetto': row['Oggetto'],
                'data_ultima_assegnazione': row['Data ultima assegnazione'].strftime('%d/%m/%Y %H:%M'),
                'giorni': int(giorni),
                'is_blocked': is_blocked,
                'stato': stato,
                'misura': misura
            }
            
            # Categorizzazione basata su stato, misura e giorni
            if stato == 'AVVIATO':
                if giorni >= 30:
                    alerts[f'ente_{misura}_30_giorni'].append(alert_data)
                elif giorni >= 15:
                    alerts[f'ente_{misura}_15_giorni'].append(alert_data)
                    
            elif stato == 'IN VERIFICA' and not is_blocked:
                if giorni >= 30:
                    alerts[f'verifica_{misura}_30_giorni'].append(alert_data)
                elif giorni >= 15:
                    alerts[f'verifica_{misura}_15_giorni'].append(alert_data)
        
        # Log dei risultati
        total_alerts = sum(len(v) for v in alerts.values())
        logger.info(f"Generati {total_alerts} alert categorizzati per misura")
        
        for category, items in alerts.items():
            if items:
                logger.info(f"  {category}: {len(items)} alert")
        
        return alerts
    
    def generate_secure_html_email(self, alerts: Dict[str, List[Dict]]) -> str:
        """Genera email con due tabelle separate per misura 1.2 e 1.4.1"""
        today = datetime.now().strftime('%d/%m/%Y')
        
        # Raccoglie alert per misura
        alerts_1_2 = []
        alerts_1_4_1 = []
        
        # Enti da contattare 1.2
        for alert in alerts['ente_1_2_15_giorni'] + alerts['ente_1_2_30_giorni']:
            alert['tipo_azione'] = 'CONTATTARE ENTE'
            alert['raccomandazione'] = f'L\'ente non ha ancora risposto al tuo esito parziale da {alert["giorni"]} giorni, contattalo al più presto'
            alerts_1_2.append(alert)
        
        # Verifiche interne 1.2
        for alert in alerts['verifica_1_2_15_giorni'] + alerts['verifica_1_2_30_giorni']:
            alert['tipo_azione'] = 'PROCEDERE CON VERIFICA'
            base_text = f'Candidatura dell\'ente in attesa di asseverazione, esito parziale invariato da {alert["giorni"]} giorni. Valuta di completare la verifica salvo presenza di blocchi ACN o istruttorie'
            if alert['is_blocked']:
                alert['raccomandazione'] = base_text + ' ⛔ BLOCCATO'
            else:
                alert['raccomandazione'] = base_text
            alerts_1_2.append(alert)
        
        # Enti da contattare 1.4.1
        for alert in alerts['ente_1_4_1_15_giorni'] + alerts['ente_1_4_1_30_giorni']:
            alert['tipo_azione'] = 'CONTATTARE ENTE'
            alert['raccomandazione'] = f'L\'ente non ha ancora risposto al tuo esito parziale da {alert["giorni"]} giorni, contattalo al più presto'
            alerts_1_4_1.append(alert)
        
        # Verifiche interne 1.4.1
        for alert in alerts['verifica_1_4_1_15_giorni'] + alerts['verifica_1_4_1_30_giorni']:
            alert['tipo_azione'] = 'PROCEDERE CON VERIFICA'
            base_text = f'Candidatura dell\'ente in attesa di asseverazione, esito parziale invariato da {alert["giorni"]} giorni. Valuta di completare la verifica salvo presenza di blocchi ACN o istruttorie'
            if alert['is_blocked']:
                alert['raccomandazione'] = base_text + ' ⛔ BLOCCATO'
            else:
                alert['raccomandazione'] = base_text
            alerts_1_4_1.append(alert)
        
        # Ordina per urgenza (urgenti prima)
        alerts_1_2.sort(key=lambda x: x['giorni'], reverse=True)
        alerts_1_4_1.sort(key=lambda x: x['giorni'], reverse=True)
        
        total_alerts = len(alerts_1_2) + len(alerts_1_4_1)
        urgenti_total = len([a for a in alerts_1_2 + alerts_1_4_1 if a['giorni'] >= 30])
        
        if total_alerts == 0:
            return f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>✅ Monitoraggio Asseverazioni Esito Parziale - {today}</h2>
                <p><strong>Stato: TUTTO OK</strong></p>
                <p>Nessuna asseverazione in esito Parziale da oltre 15 giorni.</p>
                <p>Ottimo lavoro! Tutte le asseverazioni sono aggiornate.</p>
            </body>
            </html>
            """
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>🔔 Monitoraggio Asseverazioni Esito Parziale - {today}</h2>
            
            <div style="background-color: #f8f9fa; padding: 15px; border: 1px solid #dee2e6; margin: 20px 0;">
                <h3 style="margin-top: 0;">📊 Riepilogo</h3>
                <p><strong>Totale asseverazioni in stallo:</strong> {total_alerts}</p>
                <p><strong>Situazioni urgenti (>30gg):</strong> {urgenti_total}</p>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border: 1px solid #dee2e6; margin: 20px 0;">
                <h3 style="margin-top: 0;">🎨 Legenda Colori</h3>
                <p><span style="background-color: #ffcdd2; padding: 3px 8px;">🔴 URGENTI (>30 giorni)</span> - Priorità massima</p>
                <p><span style="background-color: #ffe0b2; padding: 3px 8px;">⚠️ ATTENZIONE (15-30 giorni)</span> - Da monitorare</p>
            </div>
        """
        
        # Tabella 1.2
        if len(alerts_1_2) > 0:
            html_content += """
            <h3>📋 Asseverazioni con Esito Parziale 1.2</h3>
            <table border="1" style="border-collapse: collapse; width: 100%; margin-bottom: 30px;">
                <tr style="background-color: #f2f2f2;">
                    <th style="padding: 8px; text-align: left;">Nome Ente</th>
                    <th style="padding: 8px; text-align: left;">ID Candidatura</th>
                    <th style="padding: 8px; text-align: center;">Giorni</th>
                    <th style="padding: 8px; text-align: left;">Azione</th>
                    <th style="padding: 8px; text-align: left;">Raccomandazione</th>
                </tr>
            """
            
            for alert in alerts_1_2:
                urgenza = alert['giorni'] >= 30
                bg_color = "#ffcdd2" if urgenza else "#ffe0b2"
                
                html_content += f"""
                <tr>
                    <td style="padding: 8px;">{alert['nome_ente']}</td>
                    <td style="padding: 8px;">{alert['funding_request']}</td>
                    <td style="padding: 8px; text-align: center; background-color: {bg_color};"><strong>{alert['giorni']}</strong></td>
                    <td style="padding: 8px;">{alert['tipo_azione']}</td>
                    <td style="padding: 8px;">{alert['raccomandazione']}</td>
                </tr>
                """
            
            html_content += "</table>"
        
        # Tabella 1.4.1
        if len(alerts_1_4_1) > 0:
            html_content += """
            <h3>📋 Asseverazioni con Esito Parziale 1.4.1</h3>
            <table border="1" style="border-collapse: collapse; width: 100%; margin-bottom: 30px;">
                <tr style="background-color: #f2f2f2;">
                    <th style="padding: 8px; text-align: left;">Nome Ente</th>
                    <th style="padding: 8px; text-align: left;">ID Candidatura</th>
                    <th style="padding: 8px; text-align: center;">Giorni</th>
                    <th style="padding: 8px; text-align: left;">Azione</th>
                    <th style="padding: 8px; text-align: left;">Raccomandazione</th>
                </tr>
            """
            
            for alert in alerts_1_4_1:
                urgenza = alert['giorni'] >= 30
                bg_color = "#ffcdd2" if urgenza else "#ffe0b2"
                
                html_content += f"""
                <tr>
                    <td style="padding: 8px;">{alert['nome_ente']}</td>
                    <td style="padding: 8px;">{alert['funding_request']}</td>
                    <td style="padding: 8px; text-align: center; background-color: {bg_color};"><strong>{alert['giorni']}</strong></td>
                    <td style="padding: 8px;">{alert['tipo_azione']}</td>
                    <td style="padding: 8px;">{alert['raccomandazione']}</td>
                </tr>
                """
            
            html_content += "</table>"
        
        html_content += f"""
            <hr style="margin: 30px 0;">
            <p style="font-size: 0.9em; color: #666; margin-top: 30px;">
                <em>Report generato automaticamente - {today}</em>
            </p>
        </body>
        </html>
        """
        
        return html_content

    def _generate_entity_stats(self, alerts: Dict[str, List[Dict]]) -> Dict:
        """METODO LEGACY - Non più utilizzato con il nuovo formato dettagliato"""
        # Questo metodo non è più necessario con il nuovo formato
        # ma lo manteniamo per compatibilità
        return {'enti': {}, 'verifiche': {}}
    
    def send_email(self, html_content: str):
        """Invia l'email di reminder"""
        try:
            today = datetime.now().strftime('%d/%m/%Y')
            subject = f"📊 Monitoraggio Asseverazioni Parziali - {today}"
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.email_mittente
            msg['To'] = ', '.join(self.email_destinatari)
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.email_mittente, self.password_email)
                server.send_message(msg)
            
            logger.info(f"Email inviata con successo a {len(self.email_destinatari)} destinatari")
            
        except Exception as e:
            logger.error(f"Errore nell'invio email: {e}")
            raise

def main():
    """Funzione principale"""
    try:
        import os  # Import spostato qui
        
        # Inizializza il manager
        reminder = AsseverazioniReminderManager()
        
        # Carica e processa i dati
        logger.info("Avvio elaborazione asseverazioni...")
        
        # Priorità: CSV locale (UNICA opzione attiva)
        csv_file_path = 'data/asseverazioni.csv'
        excel_file_path = 'data/asseverazioni.xlsx'
        # sharepoint_url = os.getenv('SHAREPOINT_URL')  # COMMENTATO per sicurezza
        
        if os.path.exists(csv_file_path):
            logger.info("✅ Usando file CSV locale...")
            df = reminder.load_csv_data(csv_file_path)
        elif os.path.exists(excel_file_path):
            logger.info("✅ Usando file Excel locale (fallback)...")
            df = reminder.load_excel_data(file_path=excel_file_path)
        # elif sharepoint_url:  # COMMENTATO
        #     logger.info("Usando file da SharePoint...")
        #     df = reminder.load_excel_data(sharepoint_url=sharepoint_url)
        else:
            raise FileNotFoundError("❌ Nessun file dati trovato. Caricare asseverazioni.csv in data/")
        df = reminder.parse_date_column(df)
        partial_df = reminder.filter_partial_assessments(df)
        
        # Genera alert
        alerts = reminder.categorize_alerts(partial_df)
        
        # Crea e invia email sicura (dati aggregati)
        html_content = reminder.generate_secure_html_email(alerts)
        
        # LOG DEL CONTENUTO EMAIL
        logger.info("=" * 80)
        logger.info("📧 CONTENUTO EMAIL GENERATO:")
        logger.info("=" * 80)
        
        # Converte HTML in testo leggibile per il log
        import re
        
        # Rimuove tag HTML per una versione text-only
        text_content = re.sub(r'<[^>]+>', '', html_content)
        # Pulisce spazi multipli e newline
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        # Ripristina alcune interruzioni di riga logiche
        text_content = text_content.replace('🔔 Monitoraggio Asseverazioni PNRR', '\n🔔 Monitoraggio Asseverazioni PNRR')
        text_content = text_content.replace('📊 Riepilogo Esecutivo', '\n\n📊 Riepilogo Esecutivo')
        text_content = text_content.replace('📞 Enti da Contattare', '\n\n📞 Enti da Contattare')
        text_content = text_content.replace('⚡ Verifiche Interne', '\n\n⚡ Verifiche Interne')
        text_content = text_content.replace('💡 Raccomandazioni', '\n\n💡 Raccomandazioni')
        text_content = text_content.replace('Nota sulla Privacy:', '\n\nNota sulla Privacy:')
        
        logger.info(text_content)
        
        logger.info("=" * 80)
        logger.info("📧 CONTENUTO HTML COMPLETO:")
        logger.info("=" * 80)
        logger.info(html_content)
        logger.info("=" * 80)
        
        # Tenta l'invio email Gmail - RIABILITATO
        try:
            logger.info("📧 Tentativo invio email via Gmail...")
            reminder.send_email(html_content)
            logger.info("✅ Email inviata con successo!")
        except Exception as email_error:
            logger.warning(f"⚠️ Invio email fallito: {email_error}")
            logger.info("💡 Email non inviata, ma contenuto generato correttamente (vedi log sopra)")
        
        # Statistiche finali
        total_alerts = sum(len(v) for v in alerts.values())
        print(f"✅ Elaborazione completata!")
        print(f"📊 Totale asseverazioni analizzate: {len(df)}")
        print(f"🟡 Asseverazioni in Parziale: {len(partial_df)}")
        print(f"🔔 Alert generati: {total_alerts}")
        
    except Exception as e:
        logger.error(f"Errore nell'esecuzione: {e}")
        print(f"❌ Errore: {e}")
        exit(1)

if __name__ == "__main__":
    main()
