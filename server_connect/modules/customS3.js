const fs = require('fs-extra');
const { S3, PutObjectCommand, GetObjectCommand, ListObjectsCommand, CopyObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const mime = require('mime-types');
const path = require('path');

function buildS3Config(options, ctx) {
    const accessKeyId = ctx.parseRequired(options.accessKeyId, 'string', 'AccessKeyId is required.');
    const secretAccessKey = ctx.parseRequired(options.secretAccessKey, 'string', 'SecretAccessKey is required.');
    const region = ctx.parseOptional(options.region, 'string', 'us-east-1');
    const provider = ctx.parseOptional(options.provider, 'string', 'aws');
    const endpointOption = ctx.parseOptional(options.endpoint, 'string', '');
    const forcePathStyle = ctx.parseOptional(options.forcePathStyle, 'boolean', false);

    const config = {
        region,
        credentials: { accessKeyId, secretAccessKey },
        forcePathStyle,
        endpoint: `https://s3.${region}.amazonaws.com`
    };

    if (provider === "custom" && endpointOption) {
        config.endpoint = endpointOption;
    }

    return { config, region, provider, forcePathStyle };
}

function getS3Client(options, ctx) {
  const { config } = buildS3Config(options, ctx);
  return new S3(config);
}

function buildFileUrl({ endpoint, bucket, key, region, provider, forcePathStyle }) {
  if (forcePathStyle || provider === "custom") {
      return `${endpoint.replace(/\/$/, '')}/${bucket}/${encodeURIComponent(key)}`;
  }
  return `https://${bucket}.s3.${region}.amazonaws.com/${encodeURIComponent(key)}`;
}

exports.s3_signed_upload = async function (options) {
  const Bucket = this.parseRequired(options.bucket, 'string', 'Bucket is required.');
  const Key = this.parseRequired(options.key, 'string', 'Key is required.');
  const ContentType = this.parseOptional(options.contentType, 'string', mime.lookup(Key) || 'application/octet-stream');
  const expiresIn = this.parseOptional(options.expires, 'number', 300);
  const ACL = this.parseOptional(options.acl, 'string',undefined);

  const s3 = getS3Client(options, this);
  const command = new PutObjectCommand({ Bucket, Key, ContentType, ACL });

  return getSignedUrl(s3, command, { expiresIn });
};

exports.s3_signed_download = async function (options) {
  const Bucket = this.parseRequired(options.bucket, 'string', 'Bucket is required.');
  const Key = this.parseRequired(options.key, 'string', 'Key is required.');
  const expiresIn = this.parseOptional(options.expires, 'number', 300);

  const s3 = getS3Client(options, this);
  const command = new GetObjectCommand({ Bucket, Key });

  return getSignedUrl(s3, command, { expiresIn });
};

exports.s3_put_object = async function (options) {
  const File = this.parseRequired(options.file, 'string');
  const Bucket = this.parseRequired(options.bucket, 'string', 'Bucket is required.');
  const Key = this.parseRequired(options.key, 'string', 'Key is required.');
  const ContentType = this.parseOptional(options.contentType, 'string', mime.lookup(Key) || 'application/octet-stream');
  const ACL = this.parseOptional(options.acl, 'string');
  const ContentDisposition = this.parseOptional(options.contentDisposition, 'string');
  const useFilePath = this.parseOptional(options.useFilePath, 'boolean', false);

  const { config, region, provider, forcePathStyle } = buildS3Config(options, this);
  const s3 = new S3(config);

  let filePath = useFilePath
      ? path.join(process.cwd(), File)
      : this.req.files[File].tempFilePath;

  const Body = fs.createReadStream(filePath);

  const command = new PutObjectCommand({
      Bucket,
      Key,
      ContentType,
      ACL,
      ContentDisposition,
      Body
  });

  const result = await s3.send(command);

  return {
      ...result,
      url: buildFileUrl({
          endpoint: config.endpoint,
          bucket: Bucket,
          key: Key,
          region,
          provider,
          forcePathStyle
      }),
      bucket: Bucket,
      key: Key
  };
};

exports.s3_list_files = async function (options) {
  const Bucket = this.parseRequired(options.bucket, 'string', 'Bucket is required.');
  const Prefix = this.parseOptional(options.prefix, 'string', '');

  const { config, region, provider, forcePathStyle } = buildS3Config(options, this);
  const s3 = new S3(config);

  const result = await s3.send(new ListObjectsCommand({ Bucket, Prefix }));
  if (!result.Contents) return [];

  return result.Contents.map(file => ({
      key: file.Key,
      size: file.Size,
      lastModified: file.LastModified,
      etag: file.ETag,
      url: buildFileUrl({
          endpoint: config.endpoint,
          bucket: Bucket,
          key: file.Key,
          region,
          provider,
          forcePathStyle
      })
  }));
};

exports.s3_copy_object = async function (options) {
  const SrcBucket = this.parseRequired(options.srcBucket, 'string', 'Source Bucket is required.');
  const SrcKey = this.parseRequired(options.srcKey, 'string', 'Source Key is required.');
  const DstBucket = this.parseRequired(options.dstBucket, 'string', 'Destination Bucket is required.');
  const DstKey = this.parseRequired(options.dstKey, 'string', 'Destination Key is required.');

  const s3 = getS3Client(options, this);

  return s3.send(new CopyObjectCommand({
      Bucket: DstBucket,
      Key: DstKey,
      CopySource: `${SrcBucket}/${SrcKey}`
  }));
};

exports.s3_delete_file = async function (options) {
  const Bucket = this.parseRequired(options.bucket, 'string', 'Bucket is required.');
  const Key = this.parseRequired(options.key, 'string', 'Key is required.');

  const s3 = getS3Client(options, this);

  await s3.send(new DeleteObjectCommand({ Bucket, Key }));

  return { success: true, bucket: Bucket, key: Key };
};