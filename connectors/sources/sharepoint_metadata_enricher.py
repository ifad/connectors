import os
from typing import Dict, List, Optional, Any


class SharePointMetadataEnricher:

    def __init__(self, logger=None):
        self.logger = logger

    def _log_debug(self, message: str):
        if self.logger:
            self.logger.debug(message)

    def _log_info(self, message: str):
        if self.logger:
            self.logger.info(message)

    def _log_warning(self, message: str):
        if self.logger:
            self.logger.warning(message)

    def _extract_metadata_from_sharepoint_fields(
        self, 
        document: Dict[str, Any], 
        site: Optional[Dict[str, Any]] = None, 
        site_drive: Optional[Dict[str, Any]] = None, 
        site_list: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self._log_info(f"Starting metadata extraction for document {document.get('_id', 'unknown')}")
        
        metadata = {}
        fields = document.get("fields", {})
        
        self._log_debug(f"SharePoint fields for document {document.get('_id', 'unknown')}: {fields}")
        self._log_info(f"Found {len(fields)} SharePoint fields for document {document.get('_id', 'unknown')}")
        
        if site and site.get("webUrl"):
            site_url = site["webUrl"].lower()
            if "odc" in site_url:
                metadata["Category"] = "ODC"
                self._log_info(f"Detected ODC category from site URL: {site_url}")
            elif "xdesk" in site_url:
                metadata["Category"] = "Xdesk"
                self._log_info(f"Detected Xdesk category from site URL: {site_url}")
            else:
                metadata["Category"] = None
                self._log_info(f"No specific category detected from site URL: {site_url}")
        else:
            metadata["Category"] = None
            self._log_info("No site URL available for category detection")

        metadata["Division"] = fields.get("BusinessUnit")
        metadata["Department"] = fields.get("BusinessUnit") 
        # Document Type
        metadata["Content-Type"] = fields.get("DocumentType") or self._determine_content_type(document)
        
        # Activity and Project information
        metadata["ActivityID"] = fields.get("ActivityID")
        metadata["ActivityName"] = fields.get("ActivityName")
        metadata["ProjectID"] = fields.get("ProjectID")
        metadata["ProjectType"] = fields.get("ProjectType")
        
        # Geographic and temporal metadata
        metadata["Region"] = fields.get("Region")
        metadata["FocusCountry"] = fields.get("FocusCountry")
        metadata["Year"] = fields.get("Year")
        metadata["Phase"] = fields.get("Phase")
        
        # Status and classification
        metadata["Status"] = fields.get("OPDStatus") or fields.get("Status")
        metadata["GrantType"] = fields.get("GrantType")
        metadata["GrantWindow"] = fields.get("GrantWindow")
        
        # Boolean flags
        metadata["Disclosable"] = fields.get("Disclosable")
        metadata["NonIFAD"] = fields.get("NonIfad")
        metadata["PLF"] = fields.get("PLF")
        
        # System information
        metadata["SystemSource"] = fields.get("ODCIntegration_SystemSource")
        
        # Additional common SharePoint fields that might be present
        metadata["Title"] = fields.get("Title")
        metadata["Author"] = fields.get("Author")
        metadata["Editor"] = fields.get("Editor")
        metadata["Created"] = fields.get("Created")
        metadata["Modified"] = fields.get("Modified")
        metadata["FileLeafRef"] = fields.get("FileLeafRef")
        metadata["FileDirRef"] = fields.get("FileDirRef")
        metadata["ContentType"] = fields.get("ContentType")
        metadata["FileType"] = fields.get("File_x0020_Type")
        
        # Check for any OPD/ODC category fields
        odc_category = fields.get("OPDCategory") or fields.get("ODCCategory")
        if odc_category:
            metadata["Category"] = odc_category
            self._log_info(f"Override category with ODC field value: {odc_category}")

        self._log_info(f"Completed metadata extraction with {len(metadata)} fields for document {document.get('_id', 'unknown')}")
        return metadata

    def _determine_content_type(self, document: Dict[str, Any]) -> str:
        object_type = document.get("object_type", "")
        
        if object_type == "drive_item":
            name = document.get("name", "")
            if "folder" in document:
                return "Folder"
            elif name:
                ext = os.path.splitext(name)[-1].lower()
                if ext in ['.ppt', '.pptx']:
                    return "Presentation"
                elif ext in ['.doc', '.docx', '.pdf']:
                    return "Document"
                elif ext in ['.xls', '.xlsx']:
                    return "Spreadsheet"
                elif ext in ['.mp4', '.avi', '.mov']:
                    return "Video"
                elif ext in ['.jpg', '.jpeg', '.png', '.gif']:
                    return "Image"
                else:
                    return "Document"
            else:
                return "Document"
        elif object_type == "site_page":
            return "Web Page"
        elif object_type == "list_item":
            return "List Item"
        elif object_type == "list_item_attachment":
            return "Attachment"
        else:
            return "Document"

    def _site_path_from_web_url(self, web_url: str) -> str:
        url_parts = web_url.split("/sites/")
        site_path_parts = url_parts[1:]
        return "/sites/".join(site_path_parts)

    def build_metadata_array(
        self, 
        document: Dict[str, Any], 
        site: Optional[Dict[str, Any]] = None, 
        site_drive: Optional[Dict[str, Any]] = None, 
        site_list: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        # Build metadata array as key-value pairs for the document
        self._log_info(f"Starting to build metadata array for document {document.get('_id', 'unknown')}")
        metadata_pairs = []
        
        try:
            # Extract SharePoint-specific metadata
            sharepoint_metadata = self._extract_metadata_from_sharepoint_fields(
                document, site, site_drive, site_list
            )
            
            self._log_info(f"Building standard metadata pairs for document {document.get('_id', 'unknown')}")
            
            # Standard metadata that should always be present
            
            # Category (required field)
            metadata_pairs.append({
                "key": "Category", 
                "value": sharepoint_metadata.get("Category")
            })
            
            # Site Name
            site_name = None
            if site:
                site_name = site.get("displayName") or site.get("name") or site.get("title")
                self._log_info(f"Found site name: {site_name}")
            metadata_pairs.append({"key": "Site Name", "value": site_name})
            
            # Document Library / Drive Name
            library_name = None
            if site_drive:
                library_name = site_drive.get("name") or site_drive.get("displayName")
                self._log_info(f"Found drive library: {library_name}")
            elif site_list:
                library_name = site_list.get("name") or site_list.get("displayName")
                self._log_info(f"Found list library: {library_name}")
            metadata_pairs.append({"key": "Document Library", "value": library_name})
            
            # Division and Department (required fields)
            metadata_pairs.append({
                "key": "Division", 
                "value": sharepoint_metadata.get("Division")
            })
            metadata_pairs.append({
                "key": "Department", 
                "value": sharepoint_metadata.get("Department")
            })
            
            # Content-Type (required field)
            metadata_pairs.append({
                "key": "Content-Type", 
                "value": sharepoint_metadata.get("Content-Type")
            })
            
            # File Type/Extension
            file_extension = None
            file_name = document.get("name") or document.get("_original_filename") or document.get("FileName", "")
            if file_name and "." in file_name:
                file_extension = os.path.splitext(file_name)[-1].lower()
                self._log_info(f"Detected file extension: {file_extension} for file: {file_name}")
            metadata_pairs.append({"key": "File Type", "value": file_extension})
            
            # File Path/Location
            file_path = None
            if document.get("webUrl"):
                file_path = document["webUrl"]
                self._log_info(f"Using document webUrl as file path: {file_path}")
            elif document.get("parentReference", {}).get("path"):
                file_path = document["parentReference"]["path"]
                self._log_info(f"Using parentReference path as file path: {file_path}")
            elif site and site.get("webUrl"):
                # Construct path from site URL and document name
                site_path = self._site_path_from_web_url(site["webUrl"])
                if file_name:
                    file_path = f"{site_path}/{file_name}"
                else:
                    file_path = site_path
                self._log_info(f"Constructed file path from site URL: {file_path}")
            metadata_pairs.append({"key": "File Path", "value": file_path})
            
            # Add all SharePoint metadata fields for all documents
            self._log_info(f"Adding SharePoint-specific fields for document {document.get('_id', 'unknown')}")
            sharepoint_fields = [
                "ActivityID", "ActivityName", "ProjectID", "ProjectType",
                "Region", "FocusCountry", "Year", "Phase", "Status",
                "GrantType", "GrantWindow", "Disclosable", "NonIFAD", 
                "PLF", "SystemSource", "Title", "Author", "Editor",
                "Created", "Modified", "FileLeafRef", "FileDirRef",
                "ContentType", "FileType"
            ]
            
            added_fields_count = 0
            for field in sharepoint_fields:
                if field in sharepoint_metadata and sharepoint_metadata[field] is not None:
                    metadata_pairs.append({
                        "key": field, 
                        "value": sharepoint_metadata[field]
                    })
                    added_fields_count += 1
            
            self._log_info(f"Added {added_fields_count} SharePoint-specific fields to metadata")
            
            # Additional technical metadata
            self._log_info(f"Adding technical metadata for document {document.get('_id', 'unknown')}")
            metadata_pairs.append({"key": "Object Type", "value": document.get("object_type")})
            metadata_pairs.append({"key": "Document ID", "value": document.get("_id")})
            metadata_pairs.append({
                "key": "Last Modified", 
                "value": document.get("_timestamp") or document.get("lastModifiedDateTime")
            })
            
            # Size information for files
            if document.get("size"):
                self._log_info(f"Found file size: {document.get('size')} bytes")
                metadata_pairs.append({"key": "File Size", "value": document.get("size")})
            
            # Creator information
            created_by = None
            if document.get("createdBy", {}).get("user", {}).get("displayName"):
                created_by = document["createdBy"]["user"]["displayName"]
                self._log_info(f"Found creator display name: {created_by}")
            elif document.get("createdBy", {}).get("user", {}).get("email"):
                created_by = document["createdBy"]["user"]["email"]
                self._log_info(f"Found creator email: {created_by}")
            metadata_pairs.append({"key": "Created By", "value": created_by})
            
            # Modified by information  
            modified_by = None
            if document.get("lastModifiedBy", {}).get("user", {}).get("displayName"):
                modified_by = document["lastModifiedBy"]["user"]["displayName"]
                self._log_info(f"Found last modifier display name: {modified_by}")
            elif document.get("lastModifiedBy", {}).get("user", {}).get("email"):
                modified_by = document["lastModifiedBy"]["user"]["email"]
                self._log_info(f"Found last modifier email: {modified_by}")
            metadata_pairs.append({"key": "Modified By", "value": modified_by})
            
            self._log_info(f"Built {len(metadata_pairs)} metadata pairs for document {document.get('_id')}")
            
        except Exception as e:
            self._log_warning(f"Error building metadata array for document {document.get('_id')}: {str(e)}")
            # Return minimal metadata on error
            self._log_info("Returning minimal metadata due to error")
            metadata_pairs = [
                {"key": "Category", "value": None},
                {"key": "Site Name", "value": None},
                {"key": "Document Library", "value": None},
                {"key": "Division", "value": None},
                {"key": "Department", "value": None},
                {"key": "Content-Type", "value": None},
                {"key": "File Type", "value": None},
                {"key": "File Path", "value": None}
            ]
        
        return metadata_pairs

    def enrich_document_with_metadata(
        self, 
        document: Dict[str, Any], 
        site: Optional[Dict[str, Any]] = None, 
        site_drive: Optional[Dict[str, Any]] = None, 
        site_list: Optional[Dict[str, Any]] = None,
        enrich_metadata_enabled: bool = True
    ) -> Dict[str, Any]:

        if not enrich_metadata_enabled:
            self._log_info(f"Metadata enrichment disabled for document {document.get('_id', 'unknown')}")
            return document
            
        # Create a copy of the document to avoid modifying the original
        self._log_info(f"Starting metadata enrichment for document {document.get('_id', 'unknown')}")
        enriched_document = document.copy()
            
        try:
            metadata_array = self.build_metadata_array(enriched_document, site, site_drive, site_list)
            enriched_document["metadata"] = metadata_array
            
            self._log_info(f"Successfully enriched document {enriched_document.get('_id')} with {len(metadata_array)} metadata pairs")
            
        except Exception as e:
            self._log_warning(f"Failed to enrich document {enriched_document.get('_id')} with metadata: {str(e)}")
            # Ensure at least an empty metadata array with required fields
            self._log_info("Setting fallback metadata array with required fields")
            enriched_document["metadata"] = [
                {"key": "Category", "value": None},
                {"key": "Site Name", "value": None},
                {"key": "Document Library", "value": None},
                {"key": "Division", "value": None},
                {"key": "Department", "value": None},
                {"key": "Content-Type", "value": None},
                {"key": "File Type", "value": None},
                {"key": "File Path", "value": None}
            ]
        
        self._log_info(f"Completed metadata enrichment for document {enriched_document.get('_id', 'unknown')}")
        return enriched_document
