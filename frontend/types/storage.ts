export interface S3ObjectRef {
  bucket: string;
  key: string;
  version_id?: string;
  etag?: string;
  content_type?: string;
}
