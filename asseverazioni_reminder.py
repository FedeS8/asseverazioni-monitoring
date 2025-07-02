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
        # Configurazione email aziendale Microsoft 365
        self.email_mittente = os.getenv('EMAIL_AZIENDALE')  # f.satta@innovazione.gov.it
        self.password_email = os.getenv('PASSWORD_AZIENDALE')  # Password aziendale o App Password
        self.email_destinatari = os.getenv('EMAIL_DESTINATARI', '').split(',')
        
        # Configurazione SMTP Microsoft 365
        self.smtp_server = 'smtp.office365.com'
        self.smtp_port = 587
        
        # Configurazione SharePoint
        self.sharepoint_url = os.getenv('SHAREPOINT_URL')
        
        # Validazione configurazione
        if not all([self.email_mittente, self.password_email, self.email_destinatari[0]]):
            raise ValueError("Configurazione email aziendale incompleta. Verificare le variabili d'ambiente.")

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
        """Scarica il file Excel da SharePoint con multiple strategie"""
        try:
            import requests
            
            logger.info(f"Tentativo di download da SharePoint...")
            
            # Strategia 1: Prova download diretto con parametri
            download_urls = [
                f"{sharepoint_url}&download=1",
                f"{sharepoint_url.split('?')[0]}?download=1",
                sharepoint_url,  # Link originale
            ]
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,*/*',
                'Accept-Language': 'it-IT,it;q=0.9,en;q=0.8'
            }
            
            for i, url in enumerate(download_urls):
                try:
                    logger.info(f"Strategia {i+1}: {url[:100]}...")
                    
                    response = requests.get(url, headers=headers, allow_redirects=True, timeout=30)
                    response.raise_for_status()
                    
                    # Verifica che sia un file Excel e non HTML
                    content_type = response.headers.get('content-type', '').lower()
                    content = response.content
                    
                    logger.info(f"Content-Type: {content_type}")
                    logger.info(f"Dimensione risposta: {len(content)} bytes")
                    logger.info(f"Primi 100 caratteri: {content[:100]}")
                    
                    # Controlla se è HTML (pagina di login)
                    if content.startswith(b'<!DOCTYPE html') or content.startswith(b'<html'):
                        logger.warning(f"Strategia {i+1}: Ricevuto HTML invece di Excel (probabilmente pagina di login)")
                        continue
                    
                    # Controlla se è un file Excel valido
                    if (content.startswith(b'PK\x03\x04') or  # ZIP signature (xlsx)
                        content.startswith(b'\xd0\xcf\x11\xe0') or  # OLE signature (xls)
                        'spreadsheet' in content_type or
                        'excel' in content_type):
                        
                        # Salva il file
                        temp_file_path = 'temp_asseverazioni.xlsx'
                        with open(temp_file_path, 'wb') as f:
                            f.write(content)
                        
                        logger.info(f"✅ File Excel scaricato con successo con strategia {i+1}")
                        return temp_file_path
                    else:
                        logger.warning(f"Strategia {i+1}: Il contenuto non sembra un file Excel valido")
                        
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Strategia {i+1} fallita: {e}")
                    continue
            
            # Se tutte le strategie falliscono, solleva eccezione
            raise Exception("Tutte le strategie di download sono fallite. Il file potrebbe richiedere autenticazione.")
            
        except Exception as e:
            logger.error(f"Errore nel download da SharePoint: {e}")
            
            # Suggerimenti per l'utente
            logger.error("POSSIBILI SOLUZIONI:")
            logger.error("1. Verifica che il link di condivisione sia 'Chiunque con il collegamento può visualizzare'")
            logger.error("2. Prova a scaricare manualmente il file e caricarlo nel repository GitHub")
            logger.error("3. Considera l'uso di Microsoft Graph API per l'autenticazione")
            
            raise

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
        """Filtra solo le asseverazioni con esito Parziale"""
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
    
    def categorize_alerts(self, df: pd.DataFrame) -> Dict[str, List[Dict]]:
        """Categorizza gli alert in base allo stato e ai giorni"""
        alerts = {
            'ente_15_giorni': [],
            'ente_30_giorni': [],
            'verifica_15_giorni': [],
            'verifica_30_giorni': []
        }
        
        for _, row in df.iterrows():
            giorni = row['Giorni da ultima assegnazione']
            stato = row['Stato progetto']
            
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
                'stato': stato
            }
            
            # Categorizzazione basata su stato e giorni
            if stato == 'AVVIATO':
                if giorni >= 30:
                    alerts['ente_30_giorni'].append(alert_data)
                elif giorni >= 15:
                    alerts['ente_15_giorni'].append(alert_data)
                    
            elif stato == 'IN VERIFICA' and not is_blocked:
                if giorni >= 30:
                    alerts['verifica_30_giorni'].append(alert_data)
                elif giorni >= 15:
                    alerts['verifica_15_giorni'].append(alert_data)
        
        # Log dei risultati
        total_alerts = sum(len(v) for v in alerts.values())
        logger.info(f"Generati {total_alerts} alert categorizzati")
        
        for category, items in alerts.items():
            if items:
                logger.info(f"  {category}: {len(items)} alert")
        
        return alerts
    
    def generate_secure_html_email(self, alerts: Dict[str, List[Dict]]) -> str:
        """Genera email con dati aggregati per sicurezza aziendale"""
        today = datetime.now().strftime('%d/%m/%Y')
        
        # Calcola statistiche aggregate
        total_alerts = sum(len(v) for v in alerts.values())
        ente_total = len(alerts['ente_15_giorni']) + len(alerts['ente_30_giorni'])
        verifica_total = len(alerts['verifica_15_giorni']) + len(alerts['verifica_30_giorni'])
        urgenti_total = len(alerts['ente_30_giorni']) + len(alerts['verifica_30_giorni'])
        
        if total_alerts == 0:
            return f"""
            <html>
            <body style="font-family: Arial, sans-serif;">
                <h2>✅ Monitoraggio Asseverazioni - {today}</h2>
                <p><strong>Stato: TUTTO OK</strong></p>
                <p>Nessuna asseverazione in Parziale da oltre 15 giorni.</p>
                <p>Ottimo lavoro! Tutte le asseverazioni sono aggiornate.</p>
                
                <hr style="margin: 20px 0;">
                <p style="font-size: 0.9em; color: #666;">
                    Report automatico - Sistema di monitoraggio PNRR
                </p>
            </body>
            </html>
            """
        
        # Genera statistiche per categorie di enti (senza esporre nomi)
        enti_stats = self._generate_entity_stats(alerts)
        
        html_content = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>🔔 Monitoraggio Asseverazioni PNRR - {today}</h2>
            <p>Report settimanale delle asseverazioni che richiedono attenzione:</p>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #007acc; margin: 20px 0;">
                <h3 style="margin-top: 0; color: #007acc;">📊 Riepilogo Esecutivo</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 8px; font-weight: bold;">Totale asseverazioni da monitorare:</td>
                        <td style="padding: 8px; text-align: right;"><strong>{total_alerts}</strong></td>
                    </tr>
                    <tr style="background-color: #fff3e0;">
                        <td style="padding: 8px;">🔴 Situazioni urgenti (>30gg):</td>
                        <td style="padding: 8px; text-align: right;"><strong>{urgenti_total}</strong></td>
                    </tr>
                    <tr>
                        <td style="padding: 8px;">📞 Enti da contattare (AVVIATO):</td>
                        <td style="padding: 8px; text-align: right;">{ente_total}</td>
                    </tr>
                    <tr style="background-color: #f0f8ff;">
                        <td style="padding: 8px;">⚡ Verifiche da completare (IN VERIFICA):</td>
                        <td style="padding: 8px; text-align: right;">{verifica_total}</td>
                    </tr>
                </table>
            </div>
        """
        
        # Sezione azioni per enti (aggregata)
        if ente_total > 0:
            html_content += f"""
            <div style="margin: 20px 0;">
                <h3>📞 Enti da Contattare (AVVIATO)</h3>
                <p><strong>Azione:</strong> Stimolare risposta dagli enti</p>
                
                <table border="1" style="border-collapse: collapse; width: 100%; margin: 10px 0;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 10px;">Categoria Ente</th>
                        <th style="padding: 10px;">Progetti</th>
                        <th style="padding: 10px;">Urgenti (>30gg)</th>
                        <th style="padding: 10px;">Attenzione (15-30gg)</th>
                    </tr>
            """
            
            for categoria, stats in enti_stats['enti'].items():
                html_content += f"""
                    <tr>
                        <td style="padding: 10px;">{categoria}</td>
                        <td style="padding: 10px; text-align: center;">{stats['totale']}</td>
                        <td style="padding: 10px; text-align: center; background-color: #ffcdd2;">{stats['urgenti']}</td>
                        <td style="padding: 10px; text-align: center; background-color: #fff3e0;">{stats['normali']}</td>
                    </tr>
                """
            
            html_content += "</table></div>"
        
        # Sezione verifiche interne (aggregata)
        if verifica_total > 0:
            html_content += f"""
            <div style="margin: 20px 0;">
                <h3>⚡ Verifiche Interne da Completare</h3>
                <p><strong>Azione:</strong> Affrettare verifiche tecniche (controllare eventuali blocchi istruttori)</p>
                
                <table border="1" style="border-collapse: collapse; width: 100%; margin: 10px 0;">
                    <tr style="background-color: #f2f2f2;">
                        <th style="padding: 10px;">Categoria Ente</th>
                        <th style="padding: 10px;">Progetti</th>
                        <th style="padding: 10px;">Urgenti (>30gg)</th>
                        <th style="padding: 10px;">Attenzione (15-30gg)</th>
                        <th style="padding: 10px;">Potenzialmente Bloccati</th>
                    </tr>
            """
            
            for categoria, stats in enti_stats['verifiche'].items():
                html_content += f"""
                    <tr>
                        <td style="padding: 10px;">{categoria}</td>
                        <td style="padding: 10px; text-align: center;">{stats['totale']}</td>
                        <td style="padding: 10px; text-align: center; background-color: #ffcdd2;">{stats['urgenti']}</td>
                        <td style="padding: 10px; text-align: center; background-color: #fff3e0;">{stats['normali']}</td>
                        <td style="padding: 10px; text-align: center; background-color: #e8f5e8;">{stats['bloccati']}</td>
                    </tr>
                """
            
            html_content += "</table></div>"
        
        # Raccomandazioni strategiche
        html_content += f"""
        <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #28a745; margin: 20px 0;">
            <h3 style="margin-top: 0; color: #28a745;">💡 Raccomandazioni</h3>
            <ul>
        """
        
        if urgenti_total > 0:
            html_content += f"<li><strong>Priorità ALTA:</strong> {urgenti_total} situazioni urgenti (>30gg) richiedono intervento immediato</li>"
        
        if ente_total > 0:
            html_content += f"<li><strong>Contatti enti:</strong> Pianificare outreach per {ente_total} progetti in attesa di risposta</li>"
        
        if verifica_total > 0:
            html_content += f"<li><strong>Verifiche interne:</strong> Accelerare processo di verifica per {verifica_total} progetti</li>"
        
        html_content += """
            </ul>
        </div>
        
        <hr style="margin: 30px 0;">
        <p style="font-size: 0.9em; color: #666;">
            <strong>Nota sulla Privacy:</strong> Questo report contiene solo dati aggregati per motivi di sicurezza.<br>
            Per dettagli specifici, consultare il sistema interno di gestione asseverazioni.<br><br>
            <em>Report generato automaticamente dal sistema di monitoraggio PNRR</em>
        </p>
        </body>
        </html>
        """
        
        return html_content

    def _generate_entity_stats(self, alerts: Dict[str, List[Dict]]) -> Dict:
        """Genera statistiche aggregate per categoria di ente (senza esporre nomi specifici)"""
        stats = {
            'enti': {},
            'verifiche': {}
        }
        
        # Analizza alert per enti
        all_ente_alerts = alerts['ente_15_giorni'] + alerts['ente_30_giorni']
        self._categorize_alerts_by_type(all_ente_alerts, stats['enti'], alerts['ente_30_giorni'])
        
        # Analizza alert per verifiche
        all_verifica_alerts = alerts['verifica_15_giorni'] + alerts['verifica_30_giorni']
        self._categorize_alerts_by_type(all_verifica_alerts, stats['verifiche'], alerts['verifica_30_giorni'])
        
        return stats

    def _categorize_alerts_by_type(self, all_alerts: List[Dict], stats_dict: Dict, urgent_alerts: List[Dict]):
        """Categorizza alert per tipo di ente senza esporre dati sensibili"""
        for alert in all_alerts:
            nome_ente = alert['nome_ente'].upper()
            
            # Categorizza per tipo di ente (senza esporre nomi specifici)
            if 'COMUNE' in nome_ente:
                categoria = 'Comuni'
            elif any(word in nome_ente for word in ['ISTITUTO', 'SCUOLA', 'COMPRENSIVO']):
                categoria = 'Istituti Scolastici'
            elif any(word in nome_ente for word in ['PROVINCIA', 'REGIONE']):
                categoria = 'Enti Territoriali'
            elif any(word in nome_ente for word in ['ASL', 'OSPEDALE', 'SANITARIO']):
                categoria = 'Enti Sanitari'
            else:
                categoria = 'Altri Enti'
            
            # Inizializza categoria se non esiste
            if categoria not in stats_dict:
                stats_dict[categoria] = {
                    'totale': 0,
                    'urgenti': 0,
                    'normali': 0,
                    'bloccati': 0
                }
            
            # Aggiorna contatori
            stats_dict[categoria]['totale'] += 1
            
            # Controlla se è urgente
            is_urgent = any(urgent['funding_request'] == alert['funding_request'] for urgent in urgent_alerts)
            if is_urgent:
                stats_dict[categoria]['urgenti'] += 1
            else:
                stats_dict[categoria]['normali'] += 1
            
            # Controlla se potenzialmente bloccato
            if alert.get('is_blocked', False):
                stats_dict[categoria]['bloccati'] += 1
    
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
        
        # Priorità: CSV locale, poi SharePoint, poi Excel locale
        csv_file_path = 'data/asseverazioni.csv'
        excel_file_path = 'data/asseverazioni.xlsx'
        sharepoint_url = os.getenv('SHAREPOINT_URL')
        
        if os.path.exists(csv_file_path):
            logger.info("Usando file CSV locale...")
            df = reminder.load_csv_data(csv_file_path)
        elif sharepoint_url:
            logger.info("Usando file da SharePoint...")
            df = reminder.load_excel_data(sharepoint_url=sharepoint_url)
        elif os.path.exists(excel_file_path):
            logger.info("Usando file Excel locale...")
            df = reminder.load_excel_data(file_path=excel_file_path)
        else:
            raise FileNotFoundError("Nessun file dati trovato. Caricare asseverazioni.csv o asseverazioni.xlsx in data/")
        df = reminder.parse_date_column(df)
        partial_df = reminder.filter_partial_assessments(df)
        
        # Genera alert
        alerts = reminder.categorize_alerts(partial_df)
        
        # Crea e invia email sicura (dati aggregati)
        html_content = reminder.generate_secure_html_email(alerts)
        reminder.send_email(html_content)
        
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
