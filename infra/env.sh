export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"
export ZK_HOSTS="127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
export AWS_ACCESS_KEY_ID="hwadmin"
export AWS_SECRET_ACCESS_KEY="hwadmin-secret-2026"
export AWS_DEFAULT_REGION="us-east-1"
export AWS_ENDPOINT_URL="http://localhost:9000"
export AWS_CONFIG_FILE=/dev/null
export AWS_SHARED_CREDENTIALS_FILE=/dev/null
export AWS_PAGER=""
export NO_PROXY="*"
export CLIENT_JVMFLAGS="-Dlogback.configurationFile=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)/logback-cli.xml"
