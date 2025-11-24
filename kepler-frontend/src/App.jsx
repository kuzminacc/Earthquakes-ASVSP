import React, { useEffect, useRef } from "react";
import { Provider, useDispatch } from "react-redux";
import KeplerGl from "kepler.gl";
import { addDataToMap } from "kepler.gl/actions";
import store from "./store";

function Map() {
  const dispatch = useDispatch();
  const earthquakesRef = useRef([]);
  const fieldsRef = useRef(null);

  useEffect(() => {
    // WebSocket konekcija ka backend-u
    console.log("Attempting to connect to WebSocket at ws://localhost:8087/ws");
    const ws = new WebSocket("ws://localhost:8087/ws");

    ws.onopen = () => {
      console.log("WebSocket connected successfully!");
    };

    ws.onerror = (error) => {
      console.error("WebSocket error:", error);
    };

    ws.onclose = (event) => {
      console.log("WebSocket closed:", event.code, event.reason);
    };

    ws.onmessage = (event) => {
      console.log("Received data from WebSocket:", event.data);
      const data = JSON.parse(event.data);

      // Map field types correctly for Kepler.gl
      const getFieldType = (value) => {
        if (typeof value === "number") return "real";
        if (typeof value === "string") return "string";
        if (typeof value === "boolean") return "boolean";
        if (value instanceof Date) return "timestamp";
        return "string";
      };

      // Store fields only once (from first earthquake)
      if (!fieldsRef.current) {
        fieldsRef.current = Object.keys(data).map((k) => ({
          name: k,
          type: getFieldType(data[k]),
          format: "",
          analyzerType: getFieldType(data[k]) === "real" ? "FLOAT" : "STRING"
        }));
      }

      // Add new earthquake to accumulated list
      earthquakesRef.current.push(Object.values(data));

      // Update map with ALL earthquakes
      const keplerData = {
        info: { label: "Earthquakes", id: "earthquakes" },
        data: {
          fields: fieldsRef.current,
          rows: earthquakesRef.current,  // All earthquakes!
        },
      };

      dispatch(
        addDataToMap({
          datasets: keplerData,
          options: { centerMap: false, readOnly: false },  // Don't recenter on each update
          config: {},
        })
      );
    };

    return () => {
      console.log("Closing WebSocket connection");
      ws.close();
    };
  }, [dispatch]);

  return (
    <KeplerGl
      id="kepler"
      mapboxApiAccessToken=""
      width={window.innerWidth}
      height={window.innerHeight}
    />
  );
}

export default function App() {
  return (
    <Provider store={store}>
      <Map />
    </Provider>
  );
}