# NiFi

NiFi pulls data from external APIs and publishes it to Kafka. Flows are stored here as JSON so they can be versioned in git.

- UI: https://localhost:8443/nifi (`admin` / `travelradar123`)
- Kafka address inside NiFi: `kafka:9092`

## Flows

**`ExampleProcessGroup.json`**: fetches todos from a test API, splits the array, publishes each item to the Kafka topic `TestTodoListAPI`.

```
InvokeHTTP → SplitJson → PublishKafka → LogMessage
```

## Import

1. Drag the **Process Group** icon onto the canvas.
2. Click the upload icon, pick the JSON file, **Add**.
3. Right-click the group → **Enable all controller services** → **Start**.

## Export

Right-click the group → **Download flow definition** → **With external services**, and overwrite the file in this folder.

## Git workflow

Git only sees what you export, so the canvas and this folder are not synced automatically.

- **Push:** edit in NiFi → export → `git commit` → `git push`
- **Pull:** `git pull` → delete the old group in NiFi → import the new JSON
- One person edits a flow at a time, because the JSON is hard to merge.
