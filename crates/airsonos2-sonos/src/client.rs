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
        let response = self
            .http
            .post(url)
            .header("SOAPACTION", action.soap_action_header())
            .header("CONTENT-TYPE", r#"text/xml; charset="utf-8""#)
            .body(body)
            .send()
            .await?
            .error_for_status()?;

        Ok(response.text().await?)
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
    #[error("Sonos GetVolume response did not contain a valid CurrentVolume")]
    UnexpectedGetVolumeResponse,
}

impl SonosClientError {
    pub fn is_timeout(&self) -> bool {
        matches!(self, Self::Http(error) if error.is_timeout())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_url_formats_ipv6_literals() {
        let client = SonosClient::new("2001:db8::10".parse().expect("ipv6")).expect("client");

        assert_eq!(client.base_url().as_str(), "http://[2001:db8::10]:1400/");
    }
}
