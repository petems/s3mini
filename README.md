# s3mini

s3mini is a minimal implementation of the aws s3 CLI for s3 buckets, and concentrates on only two features: downloading files (`cp`) and listing items in an S3 bucket (`ls`)

## CLI commands

### cp

```
s3commander cp s3://hc-oss-test/go-getter/folder/main.tf .
 7 B / 7 B [=======================================] 100.00% 0s
Downloaded main.tf
```

### ls

```
s3commander ls s3://hc-oss-test/go-getter/folder/
       DIR s3://hc-oss-test/go-getter/folder/subfolder
         0 s3://hc-oss-test/go-getter/folder
         7 s3://hc-oss-test/go-getter/folder/main.tf
```

## Attribution

Original code was heavily based/forked from https://github.com/tuneinc/fasts3

## Build

make

## Testing

make test
