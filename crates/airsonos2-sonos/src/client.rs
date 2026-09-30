use std::net::IpAddr;
use std::time::Duration;

use thiserror::Error;
use url::Url;

use crate::soap::{
    SoapAction, become_standalone_body, get_volume_body, get_zone_group_state_body,
    parse_get_volume_response, pause_body, play_body, set_av_transport_uri_body,
    set_av_transport_uri_metadata, set_volume_body, stop_body,
};
use crate::sonos_addr;
use crate::topology::{ZoneGroupMember, parse_zone_group_state};

#[derive(Clone, Debug)]
pub struct SonosClient {
    base_url: Url,
    http: reqwest::Client,
}

impl SonosClient {
    pub fn new(ip: IpAddr) -> Result<Self, SonosClientError> {
        let base_url = Url::parse(&format!("http://{}", sonos_addr(ip)))?;
        Self::from_base_url(base_url)
    }

    pub fn from_base_url(base_url: Url) -> Result<Self, SonosClientError> {
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(5))
            .build()?;
        Ok(Self { base_url, http })
    }

    pub fn base_url(&self) -> &Url {
        &self.base_url
    }

    pub async fn set_av_transport_uri(
        &self,
        uri: &str,
        title: &str,
    ) -> Result<(), SonosClientError> {
        let metadata = set_av_transport_uri_metadata(title);
        self.soap(
            SoapAction::SET_AV_TRANSPORT_URI,
            set_av_transport_uri_body(uri, &metadata),
        )
        .await
    }

    pub async fn play(&self) -> Result<(), SonosClientError> {
        self.soap(SoapAction::PLAY, play_body()).await
    }

    pub async fn pause(&self) -> Result<(), SonosClientError> {
        self.soap(SoapAction::PAUSE, pause_body()).await
    }

    pub async fn stop(&self) -> Result<(), SonosClientError> {
        self.soap(SoapAction::STOP, stop_body()).await
    }

    pub async fn become_coordinator_of_standalone_group(&self) -> Result<(), SonosClientError> {
        self.soap(SoapAction::BECOME_STANDALONE, become_standalone_body())
            .await
    }

    pub async fn set_volume(&self, volume: u8) -> Result<(), SonosClientError> {
        let body = set_volume_body(volume)?;
        self.soap(SoapAction::SET_VOLUME, body).await
    }

    pub async fn get_volume(&self) -> Result<u8, SonosClientError> {
        let body = self
            .soap_with_response(SoapAction::GET_VOLUME, get_volume_body())
            .await?;
        parse_get_volume_response(&body).ok_or(SonosClientError::UnexpectedGetVolumeResponse)
    }

    pub async fn get_zone_group_state(&self) -> Result<Vec<ZoneGroupMember>, SonosClientError> {
        let body = self
            .soap_with_response(
                SoapAction::GET_ZONE_GROUP_STATE,
                get_zone_group_state_body(),
            )
            .await?;

        Ok(parse_zone_group_state(&body))
    }

    async fn soap(&self, action: SoapAction, body: String) -> Result<(), SonosClientError> {
        self.soap_with_response(action, body).await?;
        Ok(())
    }

    async fn soap_with_response(
        &self,
        action: SoapAction,
        body: String,
    ) -> Result<String, SonosClientError> {
        let url = self.base_url.join(action.service.control_path())?;
        let mut response = self
            .http
            .post(url)
            .header("SOAPACTION", action.soap_action_header())
            .header("CONTENT-TYPE", r#"text/xml; charset="utf-8""#)
            .body(body)
            .send()
            .await?;
        let status = response.status();
        let limit = if status.is_success() {
            256 * 1024
        } else {
            16 * 1024
        };
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(SonosClientError::BodyTooLarge {
                    action: action.action,
                    status: status.as_u16(),
                    limit,
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        let body = String::from_utf8_lossy(&bytes).into_owned();
        if !status.is_success() {
            return Err(SonosClientError::Fault {
                action: action.action,
                status: status.as_u16(),
                code: response_field(&body, b"errorCode").and_then(|value| value.parse().ok()),
                description: response_field(&body, b"errorDescription"),
            });
        }
        Ok(body)
    }
}

#[derive(Debug, Error)]
pub enum SonosClientError {
    #[error("failed to build Sonos URL: {0}")]
    Url(#[from] url::ParseError),
    #[error("Sonos HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("failed to build SOAP request: {0}")]
    Soap(#[from] crate::soap::SoapBuildError),
    #[error("Sonos {action} returned HTTP {status}, fault {code:?}: {description:?}")]
    Fault {
        action: &'static str,
        status: u16,
        code: Option<u16>,
        description: Option<String>,
    },
    #[error("Sonos {action} response HTTP {status} exceeded {limit} bytes")]
    BodyTooLarge {
        action: &'static str,
        status: u16,
        limit: usize,
    },
    #[error("Sonos GetVolume response did not contain a valid CurrentVolume")]
    UnexpectedGetVolumeResponse,
}

impl SonosClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Http(error) => error.is_timeout() || error.is_connect() || error.is_body(),
            Self::Fault { status, code, .. } => {
                !matches!(
                    code,
                    Some(401 | 402 | 600 | 601 | 602 | 606 | 711 | 714 | 716 | 718)
                ) && (*status == 408 || *status == 429 || *status >= 500)
            }
            _ => false,
        }
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Http(error) if error.is_timeout())
    }
}

fn response_field(xml: &str, field: &[u8]) -> Option<String> {
    let mut reader = quick_xml::Reader::from_str(xml);
    loop {
        match reader.read_event().ok()? {
            quick_xml::events::Event::Start(element) if element.local_name().as_ref() == field => {
                let text = reader.read_text(element.name()).ok()?;
                return quick_xml::escape::unescape(&text.decode().ok()?)
                    .ok()
                    .map(|value| value.into_owned());
            }
            quick_xml::events::Event::Eof => return None,
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn endpoint(
        status: axum::http::StatusCode,
        body: String,
    ) -> (SonosClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let app = axum::Router::new().fallback(move || {
            let body = body.clone();
            async move { (status, body) }
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (SonosClient::from_base_url(url).unwrap(), task)
    }

    #[tokio::test]
    async fn soap_fault_preserves_action_status_code_and_description() {
        let (client, task) = endpoint(axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "<s:Envelope xmlns:s=\"soap\"><s:Body><s:Fault><detail><UPnPError><errorCode>402</errorCode><errorDescription>Invalid &amp; missing arguments</errorDescription></UPnPError></detail></s:Fault></s:Body></s:Envelope>".into()).await;
        let error = client.play().await.unwrap_err();
        assert!(!error.is_retryable());
        assert!(
            matches!(error, SonosClientError::Fault { action: "Play", status: 500, code: Some(402), description: Some(ref description) } if description == "Invalid & missing arguments")
        );
        task.abort();
    }

    #[tokio::test]
    async fn soap_response_bodies_are_bounded() {
        for (status, limit) in [
            (axum::http::StatusCode::OK, 256 * 1024),
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, 16 * 1024),
        ] {
            let (client, task) = endpoint(status, "x".repeat(limit + 1)).await;
            assert!(
                matches!(client.play().await, Err(SonosClientError::BodyTooLarge { action: "Play", limit: actual, .. }) if actual == limit)
            );
            task.abort();
        }
    }

    #[tokio::test]
    async fn transient_server_fault_can_retry_but_redirect_is_not_followed() {
        let (client, task) =
            endpoint(axum::http::StatusCode::SERVICE_UNAVAILABLE, String::new()).await;
        assert!(client.play().await.unwrap_err().is_retryable());
        task.abort();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let app = axum::Router::new().fallback(|| async {
            axum::response::Redirect::temporary("http://127.0.0.1:1/untrusted")
        });
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = SonosClient::from_base_url(url).unwrap();
        assert!(matches!(
            client.play().await,
            Err(SonosClientError::Fault { status: 307, .. })
        ));
        task.abort();
    }

    #[test]
    fn client_url_formats_ipv6_literals() {
        let client = SonosClient::new("2001:db8::10".parse().expect("ipv6")).expect("client");

        assert_eq!(client.base_url().as_str(), "http://[2001:db8::10]:1400/");
    }
}
