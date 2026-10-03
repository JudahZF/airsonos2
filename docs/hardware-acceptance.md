# Hardware Acceptance

Use this checklist before treating a build as release-worthy.

- iPhone sees all virtual AirPlay endpoints.
- Pairings survive daemon restart.
- Apple Music plays to one Sonos room.
- Podcasts plays to one Sonos room.
- YouTube plays to one Sonos room.
- Spotify via AirPlay plays to one Sonos room.
- iPhone groups two virtual Sonos endpoints.
- iPhone groups six virtual Sonos endpoints.
- Volume changes affect the correct Sonos room.
- Disconnect stops the correct Sonos room.
- A Home Assistant Cast player plays, changes volume, and stops on disconnect.
- A Music Assistant player plays, changes volume, and stops on disconnect.
- iPhone groups a virtual Sonos endpoint with a virtual Home Assistant endpoint.
- Daemon restart recovers cleanly.
- `airsonos2 doctor` passes on the Proxmox LXC network.

For sync calibration, start with two adjacent rooms, then three, then six. Raise the configured offset for rooms that arrive late and rerun `airsonos2 calibrate --zones ...` to see the resulting delay plan.
